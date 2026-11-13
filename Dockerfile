# Stromverbrauch-App: reiner Python-Stdlop (keine pip-Abhängigkeiten).
# Bildprozess läuft als uid 1000 (Olares-Userspace-Konvention).
FROM python:3.12-slim

RUN useradd -m -u 1000 -s /bin/bash app
WORKDIR /app
COPY main.py /app/main.py
RUN chmod 644 /app/main.py

ENV DATA_DIR=/data \
    LISTEN_PORT=8080 \
    POLL_SECONDS=60 \
    TZ=Europe/Berlin

VOLUME /data
EXPOSE 8080
USER app
CMD ["python", "/app/main.py"]
