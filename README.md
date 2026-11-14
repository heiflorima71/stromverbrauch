# Stromverbrauch

Kleines Selbstbau-Dashboard für den **Janitza UMG 96-PQ-L** (Modbus TCP):
liest alle 60 Sekunden den Wirkenergie-Bezugszähler (nur lesend,
Function Code 0x03) und zeigt Verbrauch **pro Tag / Woche / Monat / Jahr**,
ein **Stundenprofil** (heute vs. gestern vs. Ø der letzten X Tage) und ein
**Netzqualitäts-Panel** mit Anomalie-Ereignissen.

## Funktionen

- Perioden-Diagramm: Tag (14 Tage), Woche (12 Wochen), Monat (mit Jahres-Auswahl), Jahr
- **Monatsarchiv**: holt die 24 rollenden Monatsregister des Zählers (double-Block
  6248/6296) alle 12 h in die eigene DB → echte Historie ab Geräte-Inbetriebnahme
- **Tagesverlauf**: kWh je Stunde, heute vs. gestern + Ø-Linie über 7/14/30 Tage
- **Heatmap**: Wochentag × Stunde, wählbar je Monat oder Ganzes Jahr (Farbskala pro Zeitraum relativ)
- **Wettervergleich**: Tages-kWh gegen Tagesmittel-Temperatur (Open-Meteo, nur abgefragt; WX_LAT/WX_LON)
- **Theme**: Auto/Dunkel/Hell (CSS-Variablen, serverseitig mitgedunkelte SVGs)
- **Netzqualität**: Spannung L1–L3, Frequenz, THD-U, Leistung je Poll;
  Anomalie-Events (Start/Extrem/Dauer) bei Grenzwertverletzung nach EN 50160
  (207–253 V, 49,5–50,5 Hz, per Env anpassbar); persistente Geräte-Extremwerte
  (Registerblöcke 3000/4000, „seit letztem Reset“)
- Versionsanzeige im Seitenkopf; `/api/status`, `/api/quality`, `/api/envlimits`
- 1 Container, reine Python-Standardbibliothek (keine pip-Abhängigkeiten)
- SQLite im App-Datenspeicher: Tabellen `readings`, `monthly_wh`, `quality`,
  `events`, `meta`
- Serverseitig gerendertes SVG (wohlgeformtes XML, keine externen Assets/CDNs)
- **Keine Schreibfunktionen gegenüber dem Zähler** (ausschließlich FC 0x03)
- Registerquellen laut offiziellem Janitza-Dokument 2.061.072.0.a:
  Energie double Reg. 6012 (Fallback float Reg. 5006), Messwerte float ab 1000,
  THD/Frequenz ab 1214, Monatsenergie 6248 (gerade Jahre) / 6296 (ungerade)

## Deployment (Olares)

Image: `ghcr.io/heiflorima71/stromverbrauch:v0.2.1` (linux/amd64).

Konfiguration über die Olares-Installationsmaske:
`MODBUS_HOST`, `MODBUS_PORT`, `MODBUS_UNIT`, `POLL_SECONDS`,
`LIMIT_U_MIN`, `LIMIT_U_MAX`, `LIMIT_F_MIN`, `LIMIT_F_MAX`.

## Bau des Images

```bash
docker build --platform=linux/amd64 \
  --label org.opencontainers.image.source="https://github.com/<owner>/stromverbrauch" \
  -t ghcr.io/<owner>/stromverbrauch:v0.1.7 app/
```

## Lizenz

MIT (Quelle: `app/main.py`).
