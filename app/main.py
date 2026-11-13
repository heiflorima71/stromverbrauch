#!/usr/bin/env python3
"""
Stromverbrauch-App — Janitza UMG 96-PQ-L via Modbus TCP.

Architektur (ein Container):
  - Sammel-Thread: liest alle POLL_SECONDS (Default 60 s) den kumulativen
    Wirkenergie-Zaehler "active energy sum, consumed" und schreibt
    (Zeitstempel, Zaehlerstand, Delta) in eine SQLite-Datenbank.
  - HTTP-Server: Seite mit Perioden-Dropdown + serverseitig gerendertes
    SVG-Balkendiagramm. Keine externen Assets/CDNs.

Registerquellen (offizielle Janitza-Liste, Doc. 2.061.072.0.a, 11/2020):
  - Standard block "Energy, type double": 6012 = active energy sum, consumed
    (double = 64 Bit IEEE 754, 4 Register, Big-Endian)
  - Fallback block "Energy, type float":  5006 = active energy sum, consumed
    (float = 32 Bit IEEE 754, 2 Register, Big-Endian)
  Double hat Vorrang (Praezision bei grossen Zaehlerstaenden).

Sicherheit: ausschliesslich Modbus FC 0x03 (Read Holding Registers).
Es werden niemals Schreibfunktionen gesendet.
"""
import html
import json
import os
import socket
import sqlite3
import struct
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# ---------------- Konfiguration (Env) ----------------
MODBUS_HOST = os.environ.get("MODBUS_HOST", "192.168.142.56")
MODBUS_PORT = int(os.environ.get("MODBUS_PORT", "502"))
MODBUS_UNIT = int(os.environ.get("MODBUS_UNIT", "1"))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8080"))
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.path.join(DATA_DIR, "energy.db")
DOUBLE_REG = int(os.environ.get("ENERGY_DOUBLE_REG", "6012"))   # double, 4 regs
FLOAT_REG = int(os.environ.get("ENERGY_FLOAT_REG", "5006"))     # float, 2 regs
READ_TIMEOUT = 5.0

_tid = 0
_tid_lock = threading.Lock()


def next_tid():
    global _tid
    with _tid_lock:
        _tid = (_tid + 1) & 0xFFFF
        return _tid


# ---------------- Modbus TCP (nur lesend, FC 0x03) ----------------
def modbus_read(host, port, unit, addr, count):
    """FC 0x03 Read Holding Registers. Gibt Liste von 16-Bit-Registern zurueck."""
    tid = next_tid()
    pdu = struct.pack(">BHH", 0x03, addr, count)
    frame = struct.pack(">HHHB", tid, 0, len(pdu) + 1, unit) + pdu
    s = socket.create_connection((host, port), timeout=READ_TIMEOUT)
    s.settimeout(READ_TIMEOUT)
    try:
        s.sendall(frame)
        head = b""
        while len(head) < 7:
            c = s.recv(7 - len(head))
            if not c:
                raise ConnectionError("Verbindung geschlossen (MBAP)")
            head += c
        rtid, _proto, length, _unit = struct.unpack(">HHHB", head)
        rest = b""
        need = length - 1
        while len(rest) < need:
            c = s.recv(need - len(rest))
            if not c:
                raise ConnectionError("Verbindung geschlossen (PDU)")
            rest += c
        if rtid != tid:
            raise ValueError("Transaktions-ID-Abweichung")
        fc = rest[0]
        if fc & 0x80:
            raise RuntimeError(f"Modbus-Ausnahme Code {rest[1] if len(rest) > 1 else 0}")
        bc = rest[1]
        return list(struct.unpack(f">{bc // 2}H", rest[2:2 + bc]))
    finally:
        s.close()


def read_energy_wh():
    """
    Liest den kumulativen Bezugs-Zaehler in Wh.
    Versucht zuerst double (6012), bei Modbus-Ausnahme float (5006).
    Rueckgabe: (wert_wh, quelle)
    """
    try:
        regs = modbus_read(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT, DOUBLE_REG, 4)
        val = struct.unpack(">d", struct.pack(">4H", *regs))[0]
        if val != val or val < 0:  # NaN / unplausibel
            raise ValueError("double-Wert unplausibel")
        return val, "double:6012"
    except RuntimeError:
        # z.B. Illegal Data Address: Firmware kennt den Double-Block nicht
        pass
    regs = modbus_read(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT, FLOAT_REG, 2)
    val = struct.unpack(">f", struct.pack(">2H", *regs))[0]
    if val != val or val < 0:
        raise ValueError("float-Wert unplausibel")
    return val, "float:5006"


# ---------------- Datenbank ----------------
def db():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    con = db()
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS readings(
            id INTEGER PRIMARY KEY,
            ts REAL NOT NULL,            -- Epoche (s)
            counter_wh REAL NOT NULL,    -- kumulativer Zaehler (Wh)
            delta_wh REAL,               -- Verbrauch seit letztem Messpunkt (Wh)
            source TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings(ts);
        CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
        """
    )
    con.commit()
    con.close()


def set_meta(k, v):
    con = db()
    con.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))
    con.commit()
    con.close()


def get_meta(k, default=""):
    con = db()
    row = con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    con.close()
    return row[0] if row else default


# ---------------- Sammel-Thread ----------------
def collector_loop():
    last_counter = None
    last_source = None
    # Vorhandener Letztwert als Basis (Neustart-fest)
    con = db()
    row = con.execute("SELECT counter_wh, source FROM readings ORDER BY ts DESC LIMIT 1").fetchone()
    con.close()
    if row:
        last_counter, last_source = row[0], row[1]

    while True:
        err = None
        try:
            wh, source = read_energy_wh()
            now = time.time()
            delta = None
            if last_counter is not None:
                if last_source != source:
                    # Quellenwechsel: Zaehlerbasis neu setzen, kein Delta
                    delta = None
                elif wh >= last_counter - 0.5:
                    delta = wh - last_counter
                else:
                    # Zaehler-Reset (Gerät zurückgesetzt): Delta = neuer Stand
                    delta = wh
            con = db()
            con.execute(
                "INSERT INTO readings(ts, counter_wh, delta_wh, source) VALUES(?,?,?,?)",
                (now, wh, delta, source),
            )
            con.commit()
            con.close()
            last_counter, last_source = wh, source
            set_meta("last_ok", str(int(now)))
            set_meta("source", source)
            set_meta("last_error", "")
        except Exception as e:  # Netzwerk/Modbus-Fehler: Protokoll + Retry
            err = f"{type(e).__name__}: {e}"
            set_meta("last_error", f"{datetime.now(timezone.utc).isoformat()} {err}")
        time.sleep(max(10, POLL_SECONDS))


# ---------------- Aggregation ----------------
def period_series(period):
    """
    Liefert (labels, values_kwh) fuer das Diagramm und Gesamt-Karten.
    Perioden: day (14 Tage), week (12 Wochen), month (12 Monate), year (alle Jahre).
    Datumsbildung in lokaler Zeit (TZ des Containers).
    """
    con = db()
    rows = con.execute(
        "SELECT ts, delta_wh FROM readings WHERE delta_wh IS NOT NULL AND delta_wh >= 0 ORDER BY ts"
    ).fetchall()
    con.close()

    now = time.time()
    buckets = {}  # sortierbarer Bucket-Key -> kwh
    for ts, delta in rows:
        dt = datetime.fromtimestamp(ts)  # lokale Zeit gemaess TZ
        if period == "day":
            key = dt.strftime("%Y-%m-%d")
        elif period == "week":
            iso = dt.isocalendar()
            key = f"{iso[0]}-W{iso[1]:02d}"
        elif period == "month":
            key = dt.strftime("%Y-%m")
        else:  # year
            key = dt.strftime("%Y")
        buckets[key] = buckets.get(key, 0.0) + delta / 1000.0

    keys = sorted(buckets.keys())
    if period == "day":
        keys = keys[-14:]
        labels = [k[8:10] + "." + k[5:7] + "." for k in keys]
    elif period == "week":
        keys = keys[-12:]
        labels = [k.replace("-", "/") for k in keys]
    elif period == "month":
        keys = keys[-12:]
        labels = [k for k in keys]
    else:
        labels = keys

    values = [round(buckets[k], 2) for k in keys]

    # Gesamt-Karten (laufende Periode)
    def bucket_key(dt):
        if period == "day":
            return dt.strftime("%Y-%m-%d")
        if period == "week":
            iso = dt.isocalendar()
            return f"{iso[0]}-W{iso[1]:02d}"
        if period == "month":
            return dt.strftime("%Y-%m")
        return dt.strftime("%Y")

    n = datetime.now()
    cards = {
        "tag": round(buckets.get(bucket_key(n), 0.0), 2),
        "woche": round(buckets.get(bucket_key(n), 0.0), 2),
        "monat": round(buckets.get(bucket_key(n), 0.0), 2),
        "jahr": round(buckets.get(bucket_key(n), 0.0), 2),
    }
    return labels, values, cards


def svg_chart(period, labels, values):
    """Serverseitig gerendertes Balkendiagramm (SVG, keine JS-Libs)."""
    titles = {"day": "Stromverbrauch je Tag (kWh)", "week": "Stromverbrauch je Woche (kWh)",
              "month": "Stromverbrauch je Monat (kWh)", "year": "Stromverbrauch je Jahr (kWh)"}
    W, H = 920, 360
    ml, mr, mt, mb = 62, 18, 42, 54
    pw, ph = W - ml - mr, H - mt - mb
    vmax = max(values) if values else 1.0
    vmax = max(vmax * 1.15, 1.0)

    def nice_step(x):
        for m in (1, 2, 5):
            for e in range(0, 10):
                if m * 10 ** e >= x:
                    return m * 10 ** e
        return x

    step = nice_step(vmax / 4)
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui,Segoe UI,Arial" font-size="12">']
    parts.append(f'<rect width="{W}" height="{H}" fill="#ffffff"/>')
    parts.append(f'<text x="{ml}" y="24" font-size="16" font-weight="600" fill="#222">{html.escape(titles[period])}</text>')

    # Gitternetz + Y-Achse
    g = 0.0
    while g <= vmax:
        y = mt + ph - (g / vmax) * ph
        parts.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{W - mr}" y2="{y:.1f}" stroke="#e6e6e6"/>')
        parts.append(f'<text x="{ml - 8}" y="{y + 4:.1f}" text-anchor="end" fill="#666">{g:g}</text>')
        g += step

    n = len(values)
    if n:
        bw = pw / n
        bar_w = max(10, bw * 0.62)
        for i, v in enumerate(values):
            x = ml + i * bw + (bw - bar_w) / 2
            hgt = (v / vmax) * ph
            y = mt + ph - hgt
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{hgt:.1f}" rx="3" fill="#2f7ed8"/>')
            parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{y - 5:.1f}" text-anchor="middle" fill="#333" font-size="10">{v:g}</text>')
            # X-Labels (bei vielen Balken ausduennen)
            skip = max(1, n // 14)
            if i % skip == 0:
                parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{mt + ph + 16}" text-anchor="middle" fill="#666" font-size="10" transform="rotate(30 {x + bar_w / 2:.1f} {mt + ph + 16})">{html.escape(labels[i])}</text>')
    parts.append(f'<text x="{ml + pw / 2:.0f}" y="{H - 6}" text-anchor="middle" fill="#888">kWh in lokaler Zeitzone</text>')
    parts.append("</svg>")
    return "".join(parts)


# ---------------- HTML ----------------
PAGE = """<!doctype html>
<html lang="de"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stromverbrauch — UMG 96-PQ-L</title>
<style>
 body{font-family:system-ui,Segoe UI,Arial,sans-serif;background:#f5f6f8;color:#222;margin:0}
 .wrap{max-width:980px;margin:0 auto;padding:24px 16px}
 h1{font-size:22px;margin:0 0 4px}
 .sub{color:#777;font-size:13px;margin-bottom:18px}
 .cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:18px}
 .card{background:#fff;border-radius:10px;padding:14px 16px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
 .card .v{font-size:22px;font-weight:700}
 .card .l{font-size:12px;color:#777}
 .panel{background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
 select{font-size:15px;padding:6px 10px;border-radius:8px;border:1px solid #ccc}
 .row{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px}
 img{width:100%;height:auto}
 .status{font-size:12px;color:#777;margin-top:10px}
 .err{color:#b00}
 @media(max-width:640px){.cards{grid-template-columns:repeat(2,1fr)}}
</style></head>
<body><div class="wrap">
 <h1>&#9889; Stromverbrauch</h1>
 <div class="sub">Janitza UMG 96-PQ-L &middot; Modbus TCP &middot; Datenpunkt alle 60&nbsp;s</div>
 <div class="cards">
  <div class="card"><div class="v" id="c-tag">–</div><div class="l">Heute (kWh)</div></div>
  <div class="card"><div class="v" id="c-woche">–</div><div class="l">Diese Woche (kWh)</div></div>
  <div class="card"><div class="v" id="c-monat">–</div><div class="l">Dieser Monat (kWh)</div></div>
  <div class="card"><div class="v" id="c-jahr">–</div><div class="l">Dieses Jahr (kWh)</div></div>
 </div>
 <div class="panel">
  <div class="row">
   <label for="p"><b>Periode:</b></label>
   <select id="p" onchange="load()">
    <option value="day" selected>Tag (14 Tage)</option>
    <option value="week">Woche (12 Wochen)</option>
    <option value="month">Monat (12 Monate)</option>
    <option value="year">Jahr (alle Jahre)</option>
   </select>
  </div>
  <img id="chart" alt="Balkendiagramm">
  <div class="status" id="status"></div>
 </div>
</div>
<script>
function load(){
  var p=document.getElementById('p').value, t=Date.now();
  document.getElementById('chart').src='/chart.svg?period='+p+'&ts='+t;
  fetch('/api/status?t='+t).then(r=>r.json()).then(s=>{
    var c=s.cards||{};
    document.getElementById('c-tag').textContent=(c.tag!=null?c.tag:'–');
    document.getElementById('c-woche').textContent=(c.woche!=null?c.woche:'–');
    document.getElementById('c-monat').textContent=(c.monat!=null?c.monat:'–');
    document.getElementById('c-jahr').textContent=(c.jahr!=null?c.jahr:'–');
    var st='Datenstand: '+(s.last_ok?('Zeitstempel '+s.last_ok):'noch kein Messpunkt')+
           (s.source?(' &middot; Quelle '+s.source):'');
    if(s.last_error) st+=' &middot; <span class=err>letzter Fehler: '+s.last_error+'</span>';
    document.getElementById('status').innerHTML=st;
  });
}
load(); setInterval(load,60000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # leise
        pass

    def _send(self, code, ctype, body):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path == "/":
                self._send(200, "text/html; charset=utf-8", PAGE)
            elif u.path == "/healthz":
                self._send(200, "text/plain", "ok")
            elif u.path == "/chart.svg":
                p = q.get("period", ["day"])[0]
                labels, values, _ = period_series(p)
                self._send(200, "image/svg+xml; charset=utf-8", svg_chart(p, labels, values))
            elif u.path == "/api/status":
                p = q.get("period", ["day"])[0]
                _, _, cards = period_series(p)
                payload = {
                    "cards": cards,
                    "last_ok": get_meta("last_ok"),
                    "source": get_meta("source"),
                    "last_error": get_meta("last_error"),
                }
                self._send(200, "application/json", json.dumps(payload))
            elif u.path == "/api/data":
                p = q.get("period", ["day"])[0]
                labels, values, cards = period_series(p)
                self._send(200, "application/json",
                           json.dumps({"period": p, "labels": labels, "values": values, "cards": cards}))
            else:
                self._send(404, "text/plain", "not found")
        except Exception as e:
            self._send(500, "text/plain", f"interner Fehler: {e}")


def main():
    init_db()
    threading.Thread(target=collector_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print(f"Stromverbrauch-App: Port {LISTEN_PORT}, Modbus {MODBUS_HOST}:{MODBUS_PORT} Unit {MODBUS_UNIT}, Poll {POLL_SECONDS}s, DB {DB_PATH}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
