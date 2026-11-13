# Stromverbrauch

Kleines Selbstbau-Dashboard für den **Janitza UMG 96-PQ-L** (Modbus TCP):
liest alle 60 Sekunden den Wirkenergie-Bezugszähler (nur lesend,
Function Code 0x03) und zeigt den Verbrauch **pro Tag / Woche / Monat / Jahr**
als Balkendiagramm mit Perioden-Auswahl.

- 1 Container, reine Python-Standardbibliothek (keine pip-Abhängigkeiten)
- SQLite-Datenbank im App-Datenspeicher, Persistenz über Neustarts
- Serverseitig gerendertes SVG-Diagramm, keine externen Assets/CDNs
- Keine Schreibfunktionen gegenüber dem Zähler
- Registerquellen laut offiziellem Janitza-Dokument 2.061.072.0.a
  („Energy, type double“ Reg. 6012, Fallback „Energy, type float“ Reg. 5006)

## Deployment (Olares)

App als Olares-App installiert; Image:
`ghcr.io/heiflorima71/stromverbrauch:v0.1.0` (linux/amd64).

Konfiguration über die Olares-Installationsmaske:
`MODBUS_HOST`, `MODBUS_PORT`, `MODBUS_UNIT`, `POLL_SECONDS`.

## Bau des Images

```bash
docker build --platform=linux/amd64 -t ghcr.io/<owner>/stromverbrauch:v0.1.0 app/
```

## Lizenz

MIT (Quelle: `app/main.py`).
