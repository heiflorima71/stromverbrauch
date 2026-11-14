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
APP_VERSION = os.environ.get("APP_VERSION", "0.1.9")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.path.join(DATA_DIR, "energy.db")
DOUBLE_REG = int(os.environ.get("ENERGY_DOUBLE_REG", "6012"))   # double, 4 regs
FLOAT_REG = int(os.environ.get("ENERGY_FLOAT_REG", "5006"))     # float, 2 regs
READ_TIMEOUT = 5.0
# Netzgrenzen (EN 50160 / VDE-AR-N 4100, 230 V ±10 %, 50 Hz ±1 %)
LIMIT_U_MIN = float(os.environ.get("LIMIT_U_MIN", "207"))
LIMIT_U_MAX = float(os.environ.get("LIMIT_U_MAX", "253"))
LIMIT_F_MIN = float(os.environ.get("LIMIT_F_MIN", "49.5"))
LIMIT_F_MAX = float(os.environ.get("LIMIT_F_MAX", "50.5"))
# Kosten & Standby-Waechter (Nachts: Poolpumpe / Frostschutz Gewaechshaus = Normalbetrieb,
# daher Standardmaessig DEAKTIVIERT; Alarm erst bei konfigurierter Grenze)
EUR_PER_KWH = float(os.environ.get("EUR_PER_KWH", "0.35"))
NIGHT_POWER_LIMIT_W = float(os.environ.get("NIGHT_POWER_LIMIT_W", "0"))  # 0 = aus
NIGHT_START_HOUR = int(os.environ.get("NIGHT_START_HOUR", "2"))
NIGHT_END_HOUR = int(os.environ.get("NIGHT_END_HOUR", "5"))
# Wettervergleich (Open-Meteo, nur ausgehender GET mit Koordinaten; 0/0 = deaktiviert)
WX_LAT = float(os.environ.get("WX_LAT", "0"))
WX_LON = float(os.environ.get("WX_LON", "0"))

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
        data = rest[2:2 + bc]
        if len(data) < bc:
            raise ValueError(f"Antwort zu kurz: {len(data)}/{bc} Bytes")
        return list(struct.unpack(f">{bc // 2}H", data))
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
        CREATE TABLE IF NOT EXISTS monthly_wh(
            ym TEXT PRIMARY KEY,          -- 'YYYY-MM'
            wh REAL NOT NULL,             -- Monatsverbrauch laut Geraetespeicher (Wh)
            source TEXT,
            updated REAL
        );
        CREATE TABLE IF NOT EXISTS quality(
            ts REAL PRIMARY KEY,
            u1 REAL, u2 REAL, u3 REAL,
            i1 REAL, i2 REAL, i3 REAL,
            p REAL, f REAL, thd_u REAL
        );
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY,
            ts_start REAL, ts_end REAL,
            kind TEXT, direction TEXT,     -- 'below'|'above'
            extrem REAL, last_value REAL,
            closed INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS weather(
            ymd TEXT PRIMARY KEY,          -- 'YYYY-MM-DD'
            tmin REAL, tmean REAL, tmax REAL,
            fetched REAL
        );
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
            print(f"{datetime.now().isoformat(timespec='seconds')} poll ok: {wh/1000:.3f} kWh gesamt, "
                  f"Delta {(delta or 0)/1000:.4f} kWh, Quelle {source}", flush=True)
            # Netzqualitaet (separat, damit Energiezähler nicht davon abhängt)
            try:
                try:
                    q = read_quality()
                except (ValueError, ConnectionError):
                    time.sleep(1.0)          # Gerat kurz verschnaufen lassen
                    q = read_quality()
                con = db()
                con.execute("INSERT INTO quality(ts,u1,u2,u3,i1,i2,i3,p,f,thd_u) VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (now, q["u1"], q["u2"], q["u3"], q["i1"], q["i2"], q["i3"], q["p"], q["f"], q["thd_u"]))
                con.commit()
                con.close()
                check_anomalies(q, now)
            except Exception as e:
                set_meta("quality_error", f"{datetime.now(timezone.utc).isoformat()} {type(e).__name__}: {e}")
        except Exception as e:  # Netzwerk/Modbus-Fehler: Protokoll + Retry
            err = f"{type(e).__name__}: {e}"
            set_meta("last_error", f"{datetime.now(timezone.utc).isoformat()} {err}")
            print(f"{datetime.now().isoformat(timespec='seconds')} poll FEHLER: {err}", flush=True)
        time.sleep(max(10, POLL_SECONDS))


# ---------------- Monats-Historie aus Geraetespeicher (Backfill) ----------------
# Offizieller Janitza-Monatsspeicher "active energy (consumed), max. monthly value":
# Double 6248.. (gerades Jahr) / 6296.. (ungerades Jahr), Fallback Float 5124/5148.
def read_double_block(addr, n):
    regs = modbus_read(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT, addr, 4 * n)
    return [struct.unpack(">d", struct.pack(">4H", *regs[4 * i:4 * i + 4]))[0] for i in range(n)]


def read_float_block(addr, n):
    regs = modbus_read(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT, addr, 2 * n)
    return [struct.unpack(">f", struct.pack(">2H", regs[2 * i], regs[2 * i + 1]))[0] for i in range(n)]


def monthly_backfill_once():
    """Liest den 24-Monats-Speicher und schreibt ihn in die DB.
    Zyklus-Regel: im geraden Jahr = (gerade=tz. Jahr, ungerade=Vorjahr),
    im ungeraden Jahr = (ungerade=tz. Jahr, gerade=Vorjahr).
    Der laufende Monat wird uebersprungen (laeuft live weiter)."""
    y = datetime.now().year
    if y % 2 == 0:
        even_year, odd_year = y, y - 1
    else:
        odd_year, even_year = y, y - 1
    try:
        ev, od, src = read_double_block(6248, 12), read_double_block(6296, 12), "monthly:double"
    except RuntimeError:
        ev, od, src = read_float_block(5124, 12), read_float_block(5148, 12), "monthly:float"
    now = time.time()
    cur_ym = datetime.now().strftime("%Y-%m")
    n = 0
    con = db()
    for m in range(12):
        for year, wh in ((even_year, ev[m]), (odd_year, od[m])):
            ym = f"{year}-{m + 1:02d}"
            if wh and wh > 0 and ym != cur_ym and ym <= cur_ym:
                con.execute(
                    "INSERT INTO monthly_wh(ym, wh, source, updated) VALUES(?,?,?,?) "
                    "ON CONFLICT(ym) DO UPDATE SET wh=excluded.wh, source=excluded.source, updated=excluded.updated",
                    (ym, wh, src, now),
                )
                n += 1
    con.commit()
    con.close()
    print(f"{datetime.now().isoformat(timespec='seconds')} monthly backfill: {n} Monate importiert ({src}, Jahre {even_year}/{odd_year})", flush=True)
    set_meta("monthly_backfill_at", str(int(now)))


def monthly_backfill_loop():
    while True:
        try:
            monthly_backfill_once()
        except Exception as e:
            print(f"{datetime.now().isoformat(timespec='seconds')} monthly backfill FEHLER: {type(e).__name__}: {e}", flush=True)
        time.sleep(12 * 3600)


# ---------------- Wettervergleich (nur empfangen: Open-Meteo Archiv-API) ----------------
def fetch_weather():
    """Holt taegliche Temperatur-Min/-Mittel/-Max aus dem Open-Meteo-Archiv (HTTPS GET,
    ohne Schluessel). Es werden NUR die konfigurierten Koordinaten uebertragen, keine
    Messdaten. Speichert in weather(ymd, tmin, tmean, tmax)."""
    if not WX_LAT or not WX_LON:
        return
    from datetime import timedelta
    import urllib.request
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    url = (f"https://archive-api.open-meteo.com/v1/archive?latitude={WX_LAT}&longitude={WX_LON}"
           f"&start_date={start}&end_date={end}"
           f"&daily=temperature_2m_min,temperature_2m_mean,temperature_2m_max&timezone=Europe%2FBerlin")
    with urllib.request.urlopen(url, timeout=20) as r:
        data = json.load(r)
    d = data.get("daily") or {}
    days = d.get("time") or []
    con = db()
    n = 0
    for i, ymd in enumerate(days):
        tmin = (d.get("temperature_2m_min") or [None] * len(days))[i]
        tmean = (d.get("temperature_2m_mean") or [None] * len(days))[i]
        tmax = (d.get("temperature_2m_max") or [None] * len(days))[i]
        if tmean is None:
            continue
        con.execute("INSERT INTO weather(ymd,tmin,tmean,tmax,fetched) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(ymd) DO UPDATE SET tmin=excluded.tmin, tmean=excluded.tmean, "
                    "tmax=excluded.tmax, fetched=excluded.fetched",
                    (ymd, tmin, tmean, tmax, time.time()))
        n += 1
    con.commit()
    con.close()
    set_meta("weather_at", str(int(time.time())))
    print(f"{datetime.now().isoformat(timespec='seconds')} weather geladen: {n} Tage ({start}..{end})", flush=True)


def weather_loop():
    if not WX_LAT or not WX_LON:
        return
    while True:
        try:
            fetch_weather()
        except Exception as e:
            print(f"{datetime.now().isoformat(timespec='seconds')} weather FEHLER: {type(e).__name__}: {e}", flush=True)
        time.sleep(24 * 3600)


# ---------------- Netzqualitaet: Lesen, Extremwerte, Anomalien ----------------
def _f(regs, addr, base):
    """Float ab Registeradresse `addr` innerhalb eines ab `base` gelesenen Blocks.
    Ein Register pro Adresse: Index = addr - base (float belegt 2 aufeinanderfolgende)."""
    i = addr - base
    return struct.unpack(">f", struct.pack(">2H", regs[i], regs[i + 1]))[0]


def read_quality():
    """Liest dokumentierte Messwerte (float-Block ab 1000, THD/Frequenz ab 1214)."""
    regs = modbus_read(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT, 1000, 34)   # 1000..1033
    u1, u2, u3 = _f(regs, 1000, 1000), _f(regs, 1002, 1000), _f(regs, 1004, 1000)
    i1, i2, i3 = _f(regs, 1012, 1000), _f(regs, 1014, 1000), _f(regs, 1016, 1000)
    p = _f(regs, 1026, 1000)
    time.sleep(0.05)
    regs2 = modbus_read(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT, 1214, 14)  # 1214..1227
    thd_u = (_f(regs2, 1214, 1214) + _f(regs2, 1216, 1214) + _f(regs2, 1218, 1214)) / 3.0
    f = _f(regs2, 1226, 1214)
    return dict(u1=u1, u2=u2, u3=u3, i1=i1, i2=i2, i3=i3, p=p, f=f, thd_u=thd_u)


def read_device_extremes():
    """Persistente Geraet-Extremwerte: 3000..3020 max, 4000..4020 min (seit Reset).
    Robust: paarweise Einzelreads (2 Register), damit kurze Antworten nicht stören."""
    names = ["U L1-N", "U L2-N", "U L3-N", "U L1-L2", "U L2-L3", "U L3-L1",
             "I L1", "I L2", "I L3", "I Summe", "Frequenz"]

    def read_block(base):
        out = {}
        for n in names:
            try:
                regs = modbus_read(MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT, base, 2)
                out[n] = round(_f(regs, base, base), 3)
            except Exception:
                out[n] = None
            time.sleep(0.15)  # Gerat nicht mit Burst-Reads treffen
            base += 2
        return out

    data = {"max": read_block(3000), "min": read_block(4000), "ts": time.time()}
    set_meta("device_extremes", json.dumps(data))
    print(f"{datetime.now().isoformat(timespec='seconds')} device extremes geladen", flush=True)


def check_anomalies(q, ts):
    """Vergleicht mit den konfigurierten Grenzen, fuehrt die Event-Tabelle."""
    monitored = [
        ("Spannung L1-N", q["u1"], LIMIT_U_MIN, LIMIT_U_MAX),
        ("Spannung L2-N", q["u2"], LIMIT_U_MIN, LIMIT_U_MAX),
        ("Spannung L3-N", q["u3"], LIMIT_U_MIN, LIMIT_U_MAX),
        ("Frequenz", q["f"], LIMIT_F_MIN, LIMIT_F_MAX),
    ]
    # Standby-Waechter: Leistung in der Nachtstunde ueber Grenzwert (nur wenn aktiviert).
    # Ausserhalb des Fensters wird ein offenes Event geschlossen (val=0).
    if NIGHT_POWER_LIMIT_W > 0:
        hour = datetime.fromtimestamp(ts).hour
        in_night = NIGHT_START_HOUR <= hour < NIGHT_END_HOUR
        monitored.append(("Nachtverbrauch (Pumpe?)", q["p"] if in_night else 0.0, 0.0, NIGHT_POWER_LIMIT_W))
    con = db()
    for kind, val, lo, hi in monitored:
        if val is None:
            continue
        direction = "below" if val < lo else ("above" if val > hi else None)
        row = con.execute("SELECT id, extrem FROM events WHERE kind=? AND closed=0", (kind,)).fetchone()
        if direction:
            ext = val
            if row:
                ext = min(row[1], val) if direction == "below" else max(row[1], val)
                con.execute("UPDATE events SET ts_end=?, extrem=?, last_value=? WHERE id=?",
                            (ts, ext, val, row[0]))
            else:
                con.execute("INSERT INTO events(ts_start,ts_end,kind,direction,extrem,last_value,closed) "
                            "VALUES(?,?,?,?,?,?,0)", (ts, ts, kind, direction, val, val))
                print(f"{datetime.now().isoformat(timespec='seconds')} ANOMALIE {kind} {direction}: {val}", flush=True)
        elif row:
            st = con.execute("SELECT ts_start FROM events WHERE id=?", (row[0],)).fetchone()[0]
            con.execute("UPDATE events SET closed=1, ts_end=?, last_value=? WHERE id=?", (ts, val, row[0]))
            print(f"{datetime.now().isoformat(timespec='seconds')} Anomalie beendet {kind} "
                  f"(Dauer {int(ts - st)}s)", flush=True)
    con.commit()
    con.close()


def quality_snapshot():
    con = db()
    last = con.execute("SELECT ts,u1,u2,u3,i1,i2,i3,p,f,thd_u FROM quality ORDER BY ts DESC LIMIT 1").fetchone()
    events = con.execute("SELECT ts_start,ts_end,kind,direction,extrem,last_value,closed FROM events "
                         "ORDER BY ts_start DESC LIMIT 30").fetchall()
    con.close()
    return {
        "last": dict(zip(["ts", "u1", "u2", "u3", "i1", "i2", "i3", "p", "f", "thd_u"], last)) if last else None,
        "events": [dict(zip(["ts_start", "ts_end", "kind", "direction", "extrem", "last_value", "closed"], e))
                   for e in events],
        "extremes": json.loads(get_meta("device_extremes", "{}")),
    }


# ---------------- Themen (hell/dunkel) fuer SVG-Diagramme ----------------
def theme(dark):
    if dark:
        return {"bg": "#16181d", "grid": "#2a2d34", "txt": "#a8adb8", "title": "#e8eaf0",
                "sub": "#7d828c", "bar": "#5aa2f0", "bar2": "#4a4f58", "val": "#c8ccd4",
                "empty": "#232830", "c0": (35, 40, 48), "c1": (90, 162, 240)}
    return {"bg": "#ffffff", "grid": "#e6e6e6", "txt": "#666666", "title": "#222222",
            "sub": "#888888", "bar": "#2f7ed8", "bar2": "#c8ccd2", "val": "#333333",
            "empty": "#f2f2f2", "c0": (235, 240, 250), "c1": (37, 99, 216)}


# ---------------- Leistung (Lastgang) & Heatmap ----------------
def power_series_24h():
    """Letzte 24 h aus quality: 48 Halbstunden-Mittel der Wirkleistung (kW)."""
    now = time.time()
    start = now - 86400
    con = db()
    rows = con.execute("SELECT ts, p FROM quality WHERE ts >= ? AND p IS NOT NULL", (start,)).fetchall()
    con.close()
    sums, cnts = [0.0] * 48, [0] * 48
    for ts, p in rows:
        i = min(47, max(0, int((ts - start) // 1800)))
        sums[i] += p
        cnts[i] += 1
    vals = [round(sums[i] / cnts[i] / 1000.0, 3) if cnts[i] else None for i in range(48)]
    labels = [datetime.fromtimestamp(start + i * 1800).strftime("%H:%M") for i in range(48)]
    return labels, vals


def power_stats():
    """Spitzenleistung (24 h), Grundlast (Nachtminimum 7 Tage), Ø-Leistung (24 h)."""
    now = time.time()
    con = db()
    rows24 = con.execute("SELECT p FROM quality WHERE ts >= ? AND p IS NOT NULL", (now - 86400,)).fetchall()
    peak = max((p for (p,) in rows24), default=None)
    avg24 = (sum(p for (p,) in rows24) / len(rows24)) if rows24 else None
    rows7 = con.execute(
        "SELECT ts, p FROM quality WHERE ts >= ? AND p IS NOT NULL", (now - 7 * 86400,)).fetchall()
    con.close()
    grund, grund_when = None, None
    by_h = {}
    for ts, p in rows7:
        dt = datetime.fromtimestamp(ts)
        if NIGHT_START_HOUR <= dt.hour < NIGHT_END_HOUR:
            by_h.setdefault((dt.strftime("%Y-%m-%d"), dt.hour), []).append(p)
    if by_h:
        per = {k: sum(v) / len(v) for k, v in by_h.items()}
        (day, hr), grund = min(per.items(), key=lambda kv: kv[1])
        grund_when = f"{day} {hr:02d} h"
    return {"peak": round(peak / 1000.0, 2) if peak is not None else None,
            "avg24": round(avg24 / 1000.0, 2) if avg24 is not None else None,
            "grundlast": round(grund / 1000.0, 2) if grund is not None else None,
            "grundlast_when": grund_when}


def svg_power(labels, vals, stats, dark=False):
    """Linien-Diagramm der Leistung (kW) ueber die letzten 24 h."""
    th = theme(dark)
    W, H = 920, 360
    ml, mr, mt, mb = 62, 18, 42, 46
    pw, ph = W - ml - mr, H - mt - mb
    pts = [(i, v) for i, v in enumerate(vals) if v is not None]
    vmax = max([v for _, v in pts] + [1.0]) * 1.15

    def X(i):
        return ml + (i + 0.5) * pw / 48

    def Y(v):
        return mt + ph - (v / vmax) * ph

    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui,Segoe UI,Arial" font-size="12">']
    parts.append(f'<rect width="{W}" height="{H}" fill="{th["bg"]}"/>')
    parts.append(f'<text x="{ml}" y="24" font-size="16" font-weight="600" fill="{th["title"]}">Leistung letzte 24 h (kW, Halbstunden-Mittel)</text>')
    g = 0.0
    step = vmax / 4
    while g <= vmax:
        y = Y(g)
        parts.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{W - mr}" y2="{y:.1f}" stroke="{th["grid"]}"/>')
        parts.append(f'<text x="{ml - 8}" y="{y + 4:.1f}" text-anchor="end" fill="{th["txt"]}">{g:.1f}</text>')
        g += step
    if pts:
        poly = " ".join(f"{X(i):.1f},{Y(v):.1f}" for i, v in pts)
        area = f"{X(pts[0][0]):.1f},{mt + ph:.1f} " + poly + f" {X(pts[-1][0]):.1f},{mt + ph:.1f}"
        parts.append(f'<polygon points="{area}" fill="{th["bar"]}" opacity="0.18"/>')
        parts.append(f'<polyline points="{poly}" fill="none" stroke="{th["bar"]}" stroke-width="2"/>')
        if stats.get("peak") is not None:
            pi, pv = max(pts, key=lambda t: t[1])
            parts.append(f'<circle cx="{X(pi):.1f}" cy="{Y(pv):.1f}" r="4" fill="#e05555"/>')
            parts.append(f'<text x="{X(pi):.1f}" y="{Y(pv) - 8:.1f}" text-anchor="middle" fill="#e05555" font-size="11">Spitze {pv:.2f}</text>')
    for i in range(0, 48, 4):
        parts.append(f'<text x="{X(i):.1f}" y="{mt + ph + 16}" text-anchor="middle" fill="{th["txt"]}" font-size="10">{labels[i]}</text>')
    note = []
    if stats.get("avg24") is not None:
        note.append(f"Ø 24 h: {stats['avg24']} kW")
    if stats.get("peak") is not None:
        note.append(f"Spitzenleistung: {stats['peak']} kW")
    if stats.get("grundlast") is not None:
        note.append(f"Grundlast {NIGHT_START_HOUR:02d}–{NIGHT_END_HOUR:02d} h (7 T): {stats['grundlast']} kW"
                    + (f" ({stats['grundlast_when']})" if stats.get("grundlast_when") else ""))
    parts.append(f'<text x="{ml + pw / 2:.0f}" y="{H - 6}" text-anchor="middle" fill="{th["sub"]}">{" · ".join(note)}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _full_days_by_hour(rows):
    by_day = {}
    for ts, d in rows:
        dt = datetime.fromtimestamp(ts)
        by_day.setdefault(dt.date(), set()).add(dt.hour)
    return by_day


def heatmap_months():
    """Monate mit >=1 vollen (>=20 h) Tagen, neueste zuerst."""
    con = db()
    rows = con.execute("SELECT ts, delta_wh FROM readings WHERE delta_wh IS NOT NULL AND delta_wh >= 0").fetchall()
    con.close()
    return sorted({dt.strftime("%Y-%m") for dt, hs in _full_days_by_hour(rows).items() if len(hs) >= 20}, reverse=True)


def heatmap_series(ym="all"):
    """Ø kWh je (Wochentag, Stunde). ym='all' = alle vollen Tage, sonst nur 'YYYY-MM'."""
    con = db()
    rows = con.execute("SELECT ts, delta_wh FROM readings WHERE delta_wh IS NOT NULL AND delta_wh >= 0").fetchall()
    con.close()
    sums = {}
    for ts, d in rows:
        dt = datetime.fromtimestamp(ts)
        sums[(dt.weekday(), dt.hour)] = sums.get((dt.weekday(), dt.hour), 0.0) + d / 1000.0
    by_day = _full_days_by_hour(rows)
    days = [d for d, hs in by_day.items() if len(hs) >= 20 and (ym == "all" or d.strftime("%Y-%m") == ym)]
    counts = {}
    for d in days:
        counts[d.weekday()] = counts.get(d.weekday(), 0) + 1
    grid = []
    for wd in range(7):
        row = []
        for h in range(24):
            n = counts.get(wd, 0)
            row.append(round(sums.get((wd, h), 0.0) / n, 2) if n else None)
        grid.append(row)
    return grid, counts


def svg_heatmap(grid, counts, dark=False, label="Ganzes Jahr"):
    """Heatmap Wochentag x Stunde: eingefaerbte Zellen (Ø kWh)."""
    th = theme(dark)
    W, H = 920, 340
    ml, mr, mt, mb = 56, 12, 42, 44
    pw, ph = W - ml - mr, H - mt - mb
    cw, ch = pw / 24, ph / 7
    flat = [v for row in grid for v in row if v is not None]
    vmax = max(flat) if flat else 1.0
    names = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]

    def color(v):
        t = 0.0 if not vmax else min(1.0, v / vmax)
        r = int(th["c0"][0] + (th["c1"][0] - th["c0"][0]) * t)
        g = int(th["c0"][1] + (th["c1"][1] - th["c0"][1]) * t)
        b = int(th["c0"][2] + (th["c1"][2] - th["c0"][2]) * t)
        return f"#{r:02x}{g:02x}{b:02x}"

    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui,Segoe UI,Arial" font-size="12">']
    parts.append(f'<rect width="{W}" height="{H}" fill="{th["bg"]}"/>')
    nd = f" (Ø über {min(counts.values()) if counts else 0} volle Tage je Wochentag)" if counts else ""
    parts.append(f'<text x="{ml}" y="24" font-size="16" font-weight="600" fill="{th["title"]}">Wann verbraucht das Haus? kWh je Wochentag × Stunde · {html.escape(label)}{nd}</text>')
    if not flat:
        parts.append(f'<text x="{W / 2:.0f}" y="{H / 2:.0f}" text-anchor="middle" fill="{th["sub"]}">Keine vollen Tage (≥20 h) im gewählten Zeitraum – die Heatmap wächst mit.</text>')
    for wd in range(7):
        parts.append(f'<text x="{ml - 8}" y="{mt + wd * ch + ch / 2 + 4:.1f}" text-anchor="end" fill="{th["txt"]}">{names[wd]}</text>')
        for h in range(24):
            v = grid[wd][h]
            x, y = ml + h * cw, mt + wd * ch
            fill = color(v) if v is not None else th["empty"]
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{cw - 1:.1f}" height="{ch - 1:.1f}" fill="{fill}"/>')
    for h in range(0, 24, 2):
        parts.append(f'<text x="{ml + h * cw + cw / 2:.1f}" y="{mt + ph + 16}" text-anchor="middle" fill="{th["txt"]}" font-size="10">{h:02d}</text>')
    parts.append(f'<text x="{ml + pw / 2:.0f}" y="{H - 6}" text-anchor="middle" fill="{th["sub"]}">Dunkelblau = hoher Verbrauch · Nachtspitzen können Poolpumpe/Gewächshaus (Frostschutz) sein</text>')
    parts.append("</svg>")
    return "".join(parts)


# ---------------- Wettervergleich: Diagramm ----------------
def weather_series(days=90):
    """Letzte N Wettertage: Tagesmittel-Temperatur + Tages-kWh (Balken nur, wo
    Verbrauchsdaten existieren – die App sammelt live ab ihrem Start)."""
    from datetime import timedelta
    con = db()
    rows = con.execute("SELECT ts, delta_wh FROM readings WHERE delta_wh IS NOT NULL AND delta_wh >= 0").fetchall()
    wx = con.execute("SELECT ymd, tmean, tmin FROM weather ORDER BY ymd").fetchall()
    con.close()
    per_day = {}
    for ts, d in rows:
        k = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        per_day[k] = per_day.get(k, 0.0) + d / 1000.0
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return [(ymd, round(per_day[ymd], 2) if ymd in per_day else None, tmean, tmin)
            for ymd, tmean, tmin in wx if ymd >= start]


def svg_weather(series, dark=False):
    """Balken = kWh/Tag, Linie = Tagesmittel-Temperatur (°C, rechte Achse)."""
    th = theme(dark)
    W, H = 920, 360
    ml, mr, mt, mb = 62, 56, 42, 54
    pw, ph = W - ml - mr, H - mt - mb
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui,Segoe UI,Arial" font-size="12">']
    parts.append(f'<rect width="{W}" height="{H}" fill="{th["bg"]}"/>')
    if not series:
        cfg = "Wettervergleich ist deaktiviert – WX_LAT/WX_LON in der App-Konfiguration setzen." \
            if (not WX_LAT or not WX_LON) else "Wetterdaten werden geladen – bitte etwas Geduld (1×/Tag)."
        parts.append(f'<text x="{W / 2:.0f}" y="{H / 2:.0f}" text-anchor="middle" fill="{th["sub"]}">{cfg}</text>')
        parts.append("</svg>")
        return "".join(parts)
    parts.append(f'<text x="{ml}" y="24" font-size="16" font-weight="600" fill="{th["title"]}">Verbrauch vs. Außentemperatur, 90 Tage (Balken kWh · Linie Ø-Temperatur °C)</text>')
    kwh_vals = [v for _, v, _, _ in series if v is not None]
    t_vals = [t for _, _, t, _ in series if t is not None]
    kmax = max(kwh_vals + [1.0]) * 1.2
    tmin = min(t_vals + [0.0]); tmax = max(t_vals + [1.0])
    tspan = max(1.0, tmax - tmin)
    t_lo, t_hi = tmin - 0.15 * tspan, tmax + 0.15 * tspan

    def Yk(v):
        return mt + ph - (v / kmax) * ph

    def Yt(v):
        return mt + ph - ((v - t_lo) / (t_hi - t_lo)) * ph

    # Gitter + linke Achse kWh, rechte Achse °C
    g = 0.0
    step = max(0.5, kmax / 4)
    while g <= kmax:
        y = Yk(g)
        parts.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{W - mr}" y2="{y:.1f}" stroke="{th["grid"]}"/>')
        parts.append(f'<text x="{ml - 8}" y="{y + 4:.1f}" text-anchor="end" fill="{th["txt"]}">{g:.0f}</text>')
        g += step
    g = t_lo
    step = (t_hi - t_lo) / 4
    while g <= t_hi:
        parts.append(f'<text x="{W - mr + 8}" y="{Yt(g) + 4:.1f}" fill="{th["txt"]}">{g:.0f}°</text>')
        g += step
    n = len(series)
    bw = pw / n
    bar_w = max(4, bw * 0.6)
    pts = []
    hdd = 0.0
    ndays_kwh = 0
    for i, (ymd, kwh, tmean, tmin_d) in enumerate(series):
        cx = ml + i * bw + bw / 2
        if kwh:
            ndays_kwh += 1
            x = cx - bar_w / 2
            parts.append(f'<rect x="{x:.1f}" y="{Yk(kwh):.1f}" width="{bar_w:.1f}" height="{mt + ph - Yk(kwh):.1f}" rx="2" fill="{th["bar"]}"/>')
        if tmean is not None:
            pts.append(f"{cx:.1f},{Yt(tmean):.1f}")
            hdd += max(0.0, 18.0 - tmean)
        if i % max(1, n // 12) == 0:
            parts.append(f'<text x="{cx:.1f}" y="{mt + ph + 16}" text-anchor="middle" fill="{th["txt"]}" font-size="9" transform="rotate(30 {cx:.1f} {mt + ph + 16})">{ymd[8:10]}.{ymd[5:7]}.</text>')
    if len(pts) > 1:
        parts.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="#e8820c" stroke-width="2.5"/>')
    parts.append(f'<text x="{ml + pw / 2:.0f}" y="{H - 6}" text-anchor="middle" fill="{th["sub"]}">{n} Tage Temperatur · {ndays_kwh} Tage mit Verbrauchsbalken (Balken erst ab App-Start) · Heizgradtage (18-°C-Basis): {hdd:.0f} · Quelle: Open-Meteo</text>')
    parts.append("</svg>")
    return "".join(parts)


# ---------------- Aggregation ----------------
def period_series(period, year=None):
    """
    Zusammenfuehrung mit fester Prioritaetsregel:
      - laufender Monat: Live-Deltas (60 s)
      - abgeschlossene Monate: Geraetearchiv monthly_wh (authoritativ),
        sonst Live (z.B. Monate ausserhalb des 24-Monats-Geraetefensters)
      - Jahr = Summe der Monatsquellen, niemals Doppelzaehlung
      - Tag/Woche: ausschliesslich Live
    Liefert (labels, values_kWh, cards).
    """
    con = db()
    rows = con.execute(
        "SELECT ts, delta_wh FROM readings WHERE delta_wh IS NOT NULL AND delta_wh >= 0 ORDER BY ts"
    ).fetchall()
    monthly = {ym: wh / 1000.0 for ym, wh in
               con.execute("SELECT ym, wh FROM monthly_wh").fetchall()}
    con.close()

    now = datetime.now()
    cur_ym = now.strftime("%Y-%m")
    live_month, live_day, live_week = {}, {}, {}
    for ts, delta in rows:
        dt = datetime.fromtimestamp(ts)  # lokale Zeit gemaess TZ
        ym = dt.strftime("%Y-%m")
        live_month[ym] = live_month.get(ym, 0.0) + delta / 1000.0
        k_day = dt.strftime("%Y-%m-%d")
        live_day[k_day] = live_day.get(k_day, 0.0) + delta / 1000.0
        iso = dt.isocalendar()
        k_w = f"{iso[0]}-W{iso[1]:02d}"
        live_week[k_w] = live_week.get(k_w, 0.0) + delta / 1000.0

    if period == "day":
        keys = sorted(live_day.keys())[-14:]
        labels = [k[8:10] + "." + k[5:7] + "." for k in keys]
        values = [round(live_day[k], 2) for k in keys]
    elif period == "week":
        keys = sorted(live_week.keys())[-12:]
        labels = [k.replace("-", "/") for k in keys]
        values = [round(live_week[k], 2) for k in keys]
    elif period == "month":
        yy = int(year) if year else now.year
        mn = ["Jan", "Feb", "Mar", "Apr", "Mai", "Jun",
              "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"]
        labels, values = [], []
        for m in range(1, 13):
            k = f"{yy}-{m:02d}"
            v = monthly.get(k, live_month.get(k, 0.0))
            if v > 0:
                labels.append(mn[m - 1])
                values.append(round(v, 2))
    else:  # year
        merged = {}
        for ym, v in monthly.items():
            merged[ym[:4]] = merged.get(ym[:4], 0.0) + v
        for ym, v in live_month.items():
            if ym not in monthly:
                merged[ym[:4]] = merged.get(ym[:4], 0.0) + v
        keys = sorted(merged.keys())
        labels = keys
        values = [round(merged[k], 1) for k in keys]

    iso_now = now.isocalendar()
    year_card = sum(v for ym, v in monthly.items() if ym[:4] == str(now.year)) + \
        sum(v for ym, v in live_month.items() if ym[:4] == str(now.year) and ym not in monthly)
    cards = {
        "tag": round(live_day.get(now.strftime("%Y-%m-%d"), 0.0), 2),
        "woche": round(live_week.get(f"{iso_now[0]}-W{iso_now[1]:02d}", 0.0), 2),
        "monat": round(monthly.get(cur_ym, live_month.get(cur_ym, 0.0)), 2),
        "jahr": round(year_card, 2),
    }
    # Prognose (Hochrechnung laufendes Jahr)
    doy = now.timetuple().tm_yday
    if doy > 0 and year_card > 0:
        cards["prognose"] = round(year_card / doy * 365, 1)
    # Kosten (EUR) je Kennzahl
    if EUR_PER_KWH > 0:
        cards["eur_per_kwh"] = EUR_PER_KWH
        cards["eur_tag"] = round(cards["tag"] * EUR_PER_KWH, 2)
        cards["eur_woche"] = round(cards["woche"] * EUR_PER_KWH, 2)
        cards["eur_monat"] = round(cards["monat"] * EUR_PER_KWH, 2)
        cards["eur_jahr"] = round(cards["jahr"] * EUR_PER_KWH, 2)
        if "prognose" in cards:
            cards["eur_prognose"] = round(cards["prognose"] * EUR_PER_KWH, 2)
    # Vergleich Vorjahr bei Monatsansicht (labels-aligned, leer wenn nicht vorhanden)
    if period == "month":
        yy = int(year) if year else now.year
        prev = []
        for m in range(1, 13):
            k = f"{yy - 1}-{m:02d}"
            v = monthly.get(k, live_month.get(k, 0.0))
            prev.append(round(v, 2) if v > 0 else None)
        cards["prev_year"] = yy - 1
        cards["prev_values"] = prev
    return labels, values, cards


def available_years():
    con = db()
    ys = {ym[:4] for (ym,) in con.execute("SELECT ym FROM monthly_wh")}
    for (ts,) in con.execute("SELECT ts FROM readings").fetchall():
        ys.add(datetime.fromtimestamp(ts).strftime("%Y"))
    con.close()
    ys.add(str(datetime.now().year))
    return sorted(ys)


# ---------------- Tagesverlauf (Stundenprofil) ----------------
def day_profile(avg_days=7):
    """Stuendige kWh-Kurven: heute, gestern, Ø der letzten avg_days vorhandenen Tage.
    Datenbasis: 60-s-Deltas der App (Stundenhistorie vor App-Start existiert nicht)."""
    from datetime import timedelta
    con = db()
    rows = con.execute(
        "SELECT ts, delta_wh FROM readings WHERE delta_wh IS NOT NULL AND delta_wh >= 0").fetchall()
    con.close()
    by_day = {}
    for ts, d in rows:
        dt = datetime.fromtimestamp(ts)
        arr = by_day.setdefault(dt.date(), [0.0] * 24)
        arr[dt.hour] += d / 1000.0
    today = datetime.now().date()
    cur = by_day.get(today, [0.0] * 24)
    yv = by_day.get(today - timedelta(days=1))
    prev = []
    for i in range(1, avg_days + 1):
        d = today - timedelta(days=i)
        if d in by_day:
            prev.append(by_day[d])
    avg = [round(sum(a[h] for a in prev) / len(prev), 3) for h in range(24)] if prev else [0.0] * 24
    return cur, yv, avg, len(prev)


def svg_day_profile(cur, yv, avg, ndays, avg_days, dark=False):
    th = theme(dark)
    W, H = 920, 380
    ml, mr, mt, mb = 62, 18, 56, 46
    pw, ph = W - ml - mr, H - mt - mb
    vmax = max(max(cur), max(yv) if yv else 0.0, max(avg) if avg else 0.0, 0.2) * 1.2

    def y(v):
        return mt + ph - (v / vmax) * ph

    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui,Segoe UI,Arial" font-size="12">']
    parts.append(f'<rect width="{W}" height="{H}" fill="{th["bg"]}"/>')
    parts.append(f'<text x="{ml}" y="22" font-size="16" font-weight="600" fill="{th["title"]}">Stromverbrauch heute vs. gestern · Ø {avg_days} Tage (kWh je Stunde)</text>')
    # Legende
    parts.append(f'<rect x="{ml}" y="30" width="10" height="10" fill="{th["bar"]}"/><text x="{ml + 14}" y="39" fill="{th["txt"]}">Heute</text>')
    if yv:
        parts.append(f'<rect x="{ml + 66}" y="30" width="10" height="10" fill="{th["bar2"]}"/><text x="{ml + 80}" y="39" fill="{th["txt"]}">Gestern</text>')
    parts.append(f'<line x1="{ml + (150 if yv else 66)}" y1="35" x2="{ml + (180 if yv else 96)}" y2="35" stroke="#e8820c" stroke-width="2.5"/>'
                 f'<text x="{ml + (186 if yv else 102)}" y="39" fill="{th["txt"]}">Ø {avg_days} Tage ({ndays} Tage Daten)</text>')

    # Gitter + Y-Beschriftung
    g = 0.0
    step = max(0.1, vmax / 5)
    while g <= vmax:
        yy = y(g)
        parts.append(f'<line x1="{ml}" y1="{yy:.1f}" x2="{W - mr}" y2="{yy:.1f}" stroke="{th["grid"]}"/>')
        parts.append(f'<text x="{ml - 8}" y="{yy + 4:.1f}" text-anchor="end" fill="{th["txt"]}">{g:.1f}</text>')
        g += step

    slot = pw / 24
    bw = 9 if yv else 18
    for h in range(24):
        cx = ml + h * slot + slot / 2
        if cur[h] > 0:
            x = cx - (bw + (1 if yv else 0)) if yv else cx - bw / 2
            parts.append(f'<rect x="{x:.1f}" y="{y(cur[h]):.1f}" width="{bw}" height="{mt + ph - y(cur[h]):.1f}" rx="2" fill="{th["bar"]}"/>')
        if yv and yv[h] > 0:
            x = cx + (1 if yv else 0)
            parts.append(f'<rect x="{x:.1f}" y="{y(yv[h]):.1f}" width="{bw}" height="{mt + ph - y(yv[h]):.1f}" rx="2" fill="{th["bar2"]}"/>')
        if h % 2 == 0:
            parts.append(f'<text x="{cx:.1f}" y="{mt + ph + 16}" text-anchor="middle" fill="{th["txt"]}" font-size="10">{h:02d}</text>')

    # Ø-Linie
    pts = " ".join(f"{ml + h * slot + slot / 2:.1f},{y(avg[h]):.1f}" for h in range(24))
    parts.append(f'<polyline points="{pts}" fill="none" stroke="#e8820c" stroke-width="2.5"/>')
    parts.append(f'<text x="{ml + pw / 2:.0f}" y="{H - 6}" text-anchor="middle" fill="{th["sub"]}">Stunde des Tages (lokale Zeit)</text>')
    parts.append("</svg>")
    return "".join(parts)


def svg_chart(period, labels, values, year=None, prev_values=None, prev_year=None, dark=False):
    """Serverseitig gerendertes Balkendiagramm (SVG, keine JS-Libs)."""
    th = theme(dark)
    titles = {"day": "Stromverbrauch je Tag (kWh)", "week": "Stromverbrauch je Woche (kWh)",
              "month": f"Stromverbrauch je Monat {year or datetime.now().year} (kWh)",
              "year": "Stromverbrauch je Jahr (kWh)"}
    W, H = 920, 360
    ml, mr, mt, mb = 62, 18, 42, 54
    pw, ph = W - ml - mr, H - mt - mb
    all_vals = list(values) + [v for v in (prev_values or []) if v]
    vmax = max(all_vals) if all_vals else 1.0
    vmax = max(vmax * 1.15, 1.0)

    def nice_step(x):
        for m in (1, 2, 5):
            for e in range(0, 10):
                if m * 10 ** e >= x:
                    return m * 10 ** e
        return x

    step = nice_step(vmax / 4)
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui,Segoe UI,Arial" font-size="12">']
    parts.append(f'<rect width="{W}" height="{H}" fill="{th["bg"]}"/>')
    title = html.escape(titles[period])
    if prev_values and prev_year:
        title += f" · Vergleich {prev_year}"
    parts.append(f'<text x="{ml}" y="24" font-size="16" font-weight="600" fill="{th["title"]}">{title}</text>')
    if prev_values and prev_year:
        parts.append(f'<rect x="{W - 250}" y="14" width="10" height="10" fill="{th["bar"]}"/>'
                     f'<text x="{W - 236}" y="23" fill="{th["txt"]}">{year}</text>')
        parts.append(f'<rect x="{W - 180}" y="14" width="10" height="10" fill="{th["bar2"]}"/>'
                     f'<text x="{W - 166}" y="23" fill="{th["txt"]}">{prev_year}</text>')

    # Gitternetz + Y-Achse
    g = 0.0
    while g <= vmax:
        y = mt + ph - (g / vmax) * ph
        parts.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{W - mr}" y2="{y:.1f}" stroke="{th["grid"]}"/>')
        parts.append(f'<text x="{ml - 8}" y="{y + 4:.1f}" text-anchor="end" fill="{th["txt"]}">{g:g}</text>')
        g += step

    n = len(values)
    if n:
        bw = pw / n
        paired = bool(prev_values)
        bar_w = max(10, bw * (0.30 if paired else 0.62))
        for i, v in enumerate(values):
            cx = ml + i * bw + bw / 2
            x = cx - bar_w - 1 if paired else cx - bar_w / 2
            hgt = (v / vmax) * ph
            y = mt + ph - hgt
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{hgt:.1f}" rx="3" fill="{th["bar"]}"/>')
            parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{y - 5:.1f}" text-anchor="middle" fill="{th["val"]}" font-size="10">{v:g}</text>')
            if paired:
                pv = prev_values[i] if i < len(prev_values) else None
                if pv:
                    xp = cx + 1
                    hp = (pv / vmax) * ph
                    yp = mt + ph - hp
                    parts.append(f'<rect x="{xp:.1f}" y="{yp:.1f}" width="{bar_w:.1f}" height="{hp:.1f}" rx="3" fill="{th["bar2"]}"/>')
            # X-Labels (bei vielen Balken ausduennen)
            skip = max(1, n // 14)
            if i % skip == 0:
                parts.append(f'<text x="{cx:.1f}" y="{mt + ph + 16}" text-anchor="middle" fill="{th["txt"]}" font-size="10" transform="rotate(30 {cx:.1f} {mt + ph + 16})">{html.escape(labels[i])}</text>')
    parts.append(f'<text x="{ml + pw / 2:.0f}" y="{H - 6}" text-anchor="middle" fill="{th["sub"]}">kWh in lokaler Zeitzone</text>')
    parts.append("</svg>")
    return "".join(parts)


# ---------------- HTML ----------------
PAGE = """<!doctype html>
<html lang="de"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Stromverbrauch — UMG 96-PQ-L</title>
<style>
 :root{--bg:#f5f6f8;--fg:#222;--card:#fff;--muted:#777;--sel-bg:#fff;--sel-fg:#222;--sel-bd:#ccc;--th:#777;--b1:#ddd;--b2:#eee;--eu:#0a7a2a;--err:#b00;--ok:#2a8620;--bad:#b00;--sh:0 1px 3px rgba(0,0,0,.08)}
 body.dark{--bg:#0f1115;--fg:#d5d9e0;--card:#1b1e24;--muted:#8a8f99;--sel-bg:#22262d;--sel-fg:#d5d9e0;--sel-bd:#3a3f47;--th:#8a8f99;--b1:#3a3f47;--b2:#2a2d34;--eu:#5cc46a;--err:#ff7a6e;--ok:#4caf50;--bad:#ff7a6e;--sh:0 1px 3px rgba(0,0,0,.5)}
 body.light{--bg:#f5f6f8;--fg:#222;--card:#fff;--muted:#777;--sel-bg:#fff;--sel-fg:#222;--sel-bd:#ccc;--th:#777;--b1:#ddd;--b2:#eee;--eu:#0a7a2a;--err:#b00;--ok:#2a8620;--bad:#b00;--sh:0 1px 3px rgba(0,0,0,.08)}
 @media(prefers-color-scheme:dark){:root{--bg:#0f1115;--fg:#d5d9e0;--card:#1b1e24;--muted:#8a8f99;--sel-bg:#22262d;--sel-fg:#d5d9e0;--sel-bd:#3a3f47;--th:#8a8f99;--b1:#3a3f47;--b2:#2a2d34;--eu:#5cc46a;--err:#ff7a6e;--ok:#4caf50;--bad:#ff7a6e;--sh:0 1px 3px rgba(0,0,0,.5)}}
 body{font-family:system-ui,Segoe UI,Arial,sans-serif;background:var(--bg);color:var(--fg);margin:0}
 .wrap{max-width:980px;margin:0 auto;padding:24px 16px}
 h1{font-size:22px;margin:0 0 4px}
 .sub{color:var(--muted);font-size:13px;margin-bottom:18px}
 .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:18px}
 .card{background:var(--card);border-radius:10px;padding:14px 16px;box-shadow:var(--sh)}
 .card .v{font-size:22px;font-weight:700}
 .card .eu{font-size:12px;color:var(--eu);margin-top:2px}
 .card .l{font-size:12px;color:var(--muted)}
 .panel{background:var(--card);border-radius:10px;padding:16px;box-shadow:var(--sh)}
 select{font-size:15px;padding:6px 10px;border-radius:8px;border:1px solid var(--sel-bd);background:var(--sel-bg);color:var(--sel-fg)}
 .row{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px}
 img{width:100%;height:auto}
 .status{font-size:12px;color:var(--muted);margin-top:10px}
 .err{color:var(--err)}
 .ok{color:var(--ok)}
 .open,.bad{color:var(--bad)}
 @media(max-width:640px){.cards{grid-template-columns:repeat(2,1fr)}}
</style></head>
<body><div class="wrap">
 <h1>&#9889; Stromverbrauch</h1>
 <div class="sub">Janitza UMG 96-PQ-L &middot; Modbus TCP &middot; Datenpunkt alle 60&nbsp;s &middot; App-Version <b>__VERSION__</b></div>
 <div class="cards">
  <div class="card"><div class="v" id="c-tag">–</div><div class="eu" id="c-tag-eu"></div><div class="l">Heute (kWh)</div></div>
  <div class="card"><div class="v" id="c-woche">–</div><div class="eu" id="c-woche-eu"></div><div class="l">Diese Woche (kWh)</div></div>
  <div class="card"><div class="v" id="c-monat">–</div><div class="eu" id="c-monat-eu"></div><div class="l">Dieser Monat (kWh)</div></div>
  <div class="card"><div class="v" id="c-jahr">–</div><div class="eu" id="c-jahr-eu"></div><div class="l">Dieses Jahr (kWh)</div></div>
  <div class="card" id="c-prog" style="display:none"><div class="v" id="c-prognose">–</div><div class="eu" id="c-prog-eu"></div><div class="l" id="c-prog-l">Prognose (kWh)</div></div>
 </div>
 <div class="panel">
  <div class="row">
   <label for="p"><b>Periode:</b></label>
   <select id="p" onchange="onPeriod()">
    <option value="day" selected>Tag (14 Tage)</option>
    <option value="week">Woche (12 Wochen)</option>
    <option value="month">Monat</option>
    <option value="year">Jahr (alle Jahre)</option>
    <option value="profile">Tagesverlauf (Stunden)</option>
    <option value="power">Leistung (24 h)</option>
    <option value="heatmap">Heatmap (Woche × Stunde)</option>
    <option value="weather">Wettervergleich (Tag)</option>
   </select>
   <label for="hm" id="hml" style="display:none"><b>Heatmap:</b></label>
   <select id="hm" onchange="load()" style="display:none">
    <option value="all">Ganzes Jahr</option>
   </select>
   <label for="theme"><b>Theme:</b></label>
   <select id="theme" onchange="setTheme(this.value)">
    <option value="auto" selected>Auto</option>
    <option value="dark">Dunkel</option>
    <option value="light">Hell</option>
   </select>
   <label for="y" id="yl" style="display:none"><b>Jahr:</b></label>
   <select id="y" onchange="load()" style="display:none"></select>
   <label for="avg" id="avgl" style="display:none"><b>Ø über:</b></label>
   <select id="avg" onchange="load()" style="display:none">
    <option value="7" selected>7 Tage</option>
    <option value="14">14 Tage</option>
    <option value="30">30 Tage</option>
   </select>
  </div>
  <img id="chart" alt="Balkendiagramm">
  <div class="status" id="status"></div>
 </div>
 <div class="panel" style="margin-top:16px">
  <b>Netzqualität</b>
  <span class="status" id="qts"></span>
  <div class="cards" style="margin-top:10px">
   <div class="card"><div class="v" id="q-u">–</div><div class="l">Spannung L1/L2/L3 (V)</div></div>
   <div class="card"><div class="v" id="q-f">–</div><div class="l">Frequenz (Hz)</div></div>
   <div class="card"><div class="v" id="q-thd">–</div><div class="l">THD-U (Ø, %)</div></div>
   <div class="card"><div class="v" id="q-p">–</div><div class="l">Leistung (W)</div></div>
  </div>
  <div style="font-size:12px;color:#777" id="q-extrem"></div>
  <table class="ev" id="evtable">
   <thead><tr><th>Zeit</th><th>Größe</th><th>Art</th><th>Extrem</th><th>Dauer</th><th>Status</th></tr></thead>
   <tbody></tbody>
  </table>
 </div>
</div>
<style>
 table.ev{width:100%;border-collapse:collapse;font-size:12px;margin-top:10px}
 table.ev th{color:var(--th);text-align:left;font-weight:600;padding:4px 6px;border-bottom:1px solid var(--b1)}
 table.ev td{padding:4px 6px;border-bottom:1px solid var(--b2)}
</style>
<script>
function fmtTs(t){var d=new Date(t*1000);return d.toLocaleString('de-DE',{day:'2-digit',month:'2-digit',year:'2-digit',hour:'2-digit',minute:'2-digit'});}
function fmtDur(s){if(s==null)return '–';s=Math.round(s);if(s<90)return s+' s';if(s<5400)return Math.round(s/60)+' min';if(s<172800)return (s/3600).toFixed(1)+' h';return (s/86400).toFixed(1)+' d';}
function loadQuality(){
 fetch('/api/quality').then(r=>r.json()).then(s=>{
  var l=s.last||{};
  document.getElementById('qts').textContent = l.ts? (' zuletzt '+fmtTs(l.ts)) : ' noch keine Messwerte';
  document.getElementById('q-u').innerHTML = [l.u1,l.u2,l.u3].map(function(v){
    if(v==null)return '–';
    var bad = v<window.ULOX||v>window.UHI;
    return '<span class="'+(bad?'bad':'')+'">'+v.toFixed(0)+'</span>';
  }).join(' / ');
  document.getElementById('q-f').innerHTML = l.f==null?'–':'<span class="'+(l.f<window.FLOX||l.f>window.FHI?'bad':'')+'">'+l.f.toFixed(2)+'</span>';
  document.getElementById('q-thd').textContent = l.thd_u==null?'–':l.thd_u.toFixed(1);
  document.getElementById('q-p').textContent = l.p==null?'–':Math.round(l.p);
  var ex=s.extremes||{};
  if(ex.max){
   function fv(v){return (v==null)?'–':v;}
   document.getElementById('q-extrem').innerHTML='Gerät (seit letztem Reset): &nbsp; U L1-N min <b>'+fv(ex.min['U L1-N'])+'</b> / max <b>'+fv(ex.max['U L1-N'])+'</b> V &nbsp;·&nbsp; Frequenz min <b>'+fv(ex.min['Frequenz'])+'</b> / max <b>'+fv(ex.max['Frequenz'])+'</b> Hz &nbsp;·&nbsp; I L1 max <b>'+fv(ex.max['I L1'])+'</b> A';
  }
  var tb=document.querySelector('#evtable tbody'); tb.innerHTML='';
  (s.events||[]).slice(0,15).forEach(function(e){
   var dur=(e.ts_end-e.ts_start);
   var st=e.closed?'<span class=ok>beendet</span>':'<span class=open>aktiv</span>';
   tb.innerHTML+='<tr><td>'+fmtTs(e.ts_start)+'</td><td>'+e.kind+'</td><td>'+(e.direction==='below'?'unter':'über')+' Grenzwert</td><td>'+e.extrem.toFixed(2)+'</td><td>'+fmtDur(dur)+'</td><td>'+st+'</td></tr>';
  });
  if(!tb.innerHTML) tb.innerHTML='<tr><td colspan=6 style="color:#777">Keine Vorkommnisse – Netz im Grenzbereich (seit App-Start)</td></tr>';
 });
}
window.ULOX=207; window.UHI=253; window.FLOX=49.5; window.FHI=50.5; window.NIGHTL=0; window.NS=2; window.NE=5;
function pwpeak(pw){return pw.peak!=null?(pw.peak+' kW ('+(pw.avg24!=null?('Ø 24 h '+pw.avg24+' kW'):'–')+')'):'–';}
fetch('/api/envlimits').then(r=>r.json()).then(x=>{window.ULOX=x.u_min;window.UHI=x.u_max;window.FLOX=x.f_min;window.FHI=x.f_max;window.NIGHTL=x.night_limit_w;window.NS=x.night_start;window.NE=x.night_end;}).catch(()=>{});
function onPeriod(){
  var p=document.getElementById('p').value;
  document.getElementById('yl').style.display=(p==='month')?'inline':'none';
  document.getElementById('y').style.display=(p==='month')?'inline':'none';
  document.getElementById('avgl').style.display=(p==='profile')?'inline':'none';
  document.getElementById('avg').style.display=(p==='profile')?'inline':'none';
  document.getElementById('hml').style.display=(p==='heatmap')?'inline':'none';
  document.getElementById('hm').style.display=(p==='heatmap')?'inline':'none';
  load();
}
function sysDark(){return window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches;}
function themePref(){return localStorage.getItem('sv-theme')||'auto';}
function isDark(){var p=themePref();return p==='dark'||(p==='auto'&&sysDark());}
function applyTheme(){var p=themePref();document.body.classList.toggle('dark',p==='dark');document.body.classList.toggle('light',p==='light');}
function setTheme(v){localStorage.setItem('sv-theme',v);applyTheme();load();}
function load(){
  var p=document.getElementById('p').value, t=Date.now();
  var y=document.getElementById('y').value||'';
  var a=document.getElementById('avg').value||'7';
  var hm=document.getElementById('hm').value||'all';
  var extra=(p==='month'?'&year='+y:'')+(p==='profile'?'&avg='+a:'')+(p==='heatmap'?'&ym='+hm:'')+(isDark()?'&dark=1':'');
  document.getElementById('chart').src='/chart.svg?period='+p+extra+'&ts='+t;
  fetch('/api/status?period='+p+'&year='+y+'&t='+t).then(r=>r.json()).then(s=>{
    var c=s.cards||{};
    document.getElementById('c-tag').textContent=(c.tag!=null?c.tag:'–');
    document.getElementById('c-woche').textContent=(c.woche!=null?c.woche:'–');
    document.getElementById('c-monat').textContent=(c.monat!=null?c.monat:'–');
    document.getElementById('c-jahr').textContent=(c.jahr!=null?c.jahr:'–');
    function eu(id,v){document.getElementById(id).textContent=(v!=null)?('≈ '+v.toLocaleString('de-DE',{minimumFractionDigits:2,maximumFractionDigits:2})+' €'):'';}
    eu('c-tag-eu',c.eur_tag); eu('c-woche-eu',c.eur_woche); eu('c-monat-eu',c.eur_monat); eu('c-jahr-eu',c.eur_jahr);
    if(c.prognose!=null){
      document.getElementById('c-prog').style.display='block';
      document.getElementById('c-prognose').textContent=c.prognose;
      document.getElementById('c-prog-l').textContent='Prognose '+new Date().getFullYear()+' (kWh)';
      eu('c-prog-eu',c.eur_prognose);
    } else { document.getElementById('c-prog').style.display='none'; }
    var st='Datenstand: '+(s.last_ok?('Zeitstempel '+s.last_ok):'noch kein Messpunkt')+
           (s.source?(' &middot; Quelle '+s.source):'');
    if(p==='power'&&s.power){
      var pw=s.power;
      st+='<br>Spitzenleistung '+(pwpeak(pw))+ ' · Grundlast (7 T, nachts) '+(pw.grundlast!=null?pw.grundlast+' kW':'–')+
          (pw.grundlast_when?(' am '+pw.grundlast_when):'')+
          ' · <span style="color:#888">Standby-Wächter: '+(window.NIGHTL>0?('Grenze '+window.NIGHTL+' W ('+window.NS+'–'+window.NE+' h)'):'aus (Poolpumpe/Frostschutz = Normalbetrieb)')+'</span>';
    }
    if(s.last_error) st+=' &middot; <span class=err>letzter Fehler: '+s.last_error+'</span>';
    document.getElementById('status').innerHTML=st;
    if(p==='weather'){
      fetch('/api/weather').then(r=>r.json()).then(w=>{
        var t='Wetter: '+(w.configured?(w.days+' Tage geladen'+(w.fetched_at?(' · zuletzt '+new Date(w.fetched_at*1000).toLocaleString('de-DE')):' · erste Lieferung in Kürze')):'deaktiviert – WX_LAT/WX_LON in der App-Konfiguration setzen');
        document.getElementById('status').innerHTML+='<br>'+t;
      });
    }
  });
}
function initHm(){
  fetch('/api/heatmapmonths').then(r=>r.json()).then(ms=>{
    var sel=document.getElementById('hm'), cur=sel.value||'all';
    sel.innerHTML='<option value="all">Ganzes Jahr</option>'+ms.map(m=>'<option value="'+m+'">'+m.slice(5,7)+'.'+m.slice(0,4)+'</option>').join('');
    if(cur) sel.value=cur;
  });
}
function initYears(){
  fetch('/api/years').then(r=>r.json()).then(ys=>{
    var sel=document.getElementById('y');
    sel.innerHTML=ys.slice().reverse().map(y=>'<option>'+y+'</option>').join('');
    if(ys.length) sel.value=ys[ys.length-1];
  });
}
initYears(); initHm(); applyTheme();
document.getElementById('theme').value=themePref();
load(); loadQuality();
var mmdark=window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)');
if(mmdark&&mmdark.addEventListener){mmdark.addEventListener('change',function(){if(themePref()==='auto')load();});}
setInterval(()=>{load(); initYears(); initHm(); loadQuality();},60000);
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
                self._send(200, "text/html; charset=utf-8", PAGE.replace("__VERSION__", APP_VERSION))
            elif u.path == "/healthz":
                self._send(200, "text/plain", "ok")
            elif u.path == "/chart.svg":
                p = q.get("period", ["day"])[0]
                yr = q.get("year", [None])[0]
                dark = q.get("dark", ["0"])[0] == "1"
                if p == "profile":
                    nd = int(q.get("avg", ["7"])[0] or 7)
                    cur, yv, avg, n = day_profile(nd)
                    self._send(200, "image/svg+xml; charset=utf-8", svg_day_profile(cur, yv, avg, n, nd, dark))
                    return
                if p == "power":
                    labels, vals = power_series_24h()
                    self._send(200, "image/svg+xml; charset=utf-8", svg_power(labels, vals, power_stats(), dark))
                    return
                if p == "heatmap":
                    ym = q.get("ym", ["all"])[0]
                    grid, counts = heatmap_series(ym)
                    lbl = "Ganzes Jahr" if ym == "all" else (ym[5:7] + "." + ym[:4])
                    self._send(200, "image/svg+xml; charset=utf-8", svg_heatmap(grid, counts, dark, lbl))
                    return
                if p == "weather":
                    self._send(200, "image/svg+xml; charset=utf-8", svg_weather(weather_series(), dark))
                    return
                labels, values, cards = period_series(p, yr)
                self._send(200, "image/svg+xml; charset=utf-8",
                           svg_chart(p, labels, values, yr,
                                     cards.get("prev_values") if p == "month" else None,
                                     cards.get("prev_year") if p == "month" else None,
                                     dark))
            elif u.path == "/api/heatmapmonths":
                self._send(200, "application/json", json.dumps(heatmap_months()))
            elif u.path == "/api/years":
                self._send(200, "application/json", json.dumps(available_years()))
            elif u.path == "/api/quality":
                self._send(200, "application/json", json.dumps(quality_snapshot()))
            elif u.path == "/api/envlimits":
                self._send(200, "application/json", json.dumps(
                    {"u_min": LIMIT_U_MIN, "u_max": LIMIT_U_MAX,
                     "f_min": LIMIT_F_MIN, "f_max": LIMIT_F_MAX,
                     "night_limit_w": NIGHT_POWER_LIMIT_W,
                     "night_start": NIGHT_START_HOUR, "night_end": NIGHT_END_HOUR,
                     "eur_per_kwh": EUR_PER_KWH}))
            elif u.path == "/api/weather":
                con = db()
                n = con.execute("SELECT COUNT(*) FROM weather").fetchone()[0]
                con.close()
                self._send(200, "application/json", json.dumps(
                    {"configured": bool(WX_LAT and WX_LON), "days": n,
                     "fetched_at": get_meta("weather_at", "")}))
            elif u.path == "/api/status":
                p = q.get("period", ["day"])[0]
                _, _, cards = period_series(p, q.get("year", [None])[0])
                payload = {
                    "cards": cards,
                    "version": APP_VERSION,
                    "last_ok": get_meta("last_ok"),
                    "source": get_meta("source"),
                    "last_error": get_meta("last_error"),
                }
                if p == "power":
                    payload["power"] = power_stats()
                self._send(200, "application/json", json.dumps(payload))
            elif u.path == "/api/data":
                p = q.get("period", ["day"])[0]
                labels, values, cards = period_series(p, q.get("year", [None])[0])
                self._send(200, "application/json",
                           json.dumps({"period": p, "labels": labels, "values": values, "cards": cards}))
            else:
                self._send(404, "text/plain", "not found")
        except Exception as e:
            self._send(500, "text/plain", f"interner Fehler: {e}")


def main():
    init_db()
    try:
        read_device_extremes()
    except Exception as e:
        print(f"device extremes FEHLER: {e}", flush=True)
    threading.Thread(target=collector_loop, daemon=True).start()
    threading.Thread(target=monthly_backfill_loop, daemon=True).start()
    threading.Thread(target=weather_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    print(f"Stromverbrauch-App: Port {LISTEN_PORT}, Modbus {MODBUS_HOST}:{MODBUS_PORT} Unit {MODBUS_UNIT}, Poll {POLL_SECONDS}s, DB {DB_PATH}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
