# README für BFH ROOMS → Konsolidierter ICS-Feed

## Projektstruktur
```
rooms/
 ├─ ics_service.py          # Python-Skript (früher "konsolidierter Ics-feed.py")
 ├─ requirements.txt        # Python-Abhängigkeiten
 ├─ Procfile                # Render Startbefehl
 ├─ .env.example            # Beispiel-Umgebungsvariablen
```

## Lokale Nutzung
1. Virtuelle Umgebung erstellen und aktivieren:
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # Linux/Mac
   venv\Scripts\activate      # Windows
   ```

2. Abhängigkeiten installieren:
   ```bash
   pip install -r requirements.txt
   ```

3. .env-Datei anlegen (siehe .env.example) und Variablen setzen, z. B.:
   ```env
   ROOMS_BASE_URL=https://<eure-instanz>/Default
   IDP_TOKEN_URL=https://<euer-idp>/connect/token
   IDP_CLIENT_ID=rooms_api_client
   ROOMS_APIKEY_PIN=123456
   GROUP_MAP_JSON={"biel_seminar":[101,102],"bern_gruppen":[201,202]}
   DAYS_PAST=1
   DAYS_FUTURE=30
   CACHE_TTL=300
   ```

4. CLI-Test (ohne Webserver):
   ```bash
   python ics_service.py --group biel_seminar --no-run > test.ics
   ```
   → Die Datei `test.ics` kann in Outlook oder Google Calendar importiert werden.

## Deployment auf Render
1. GitHub-Repo verbinden.
2. Neuen **Web Service** anlegen.
3. Build Command:
   ```bash
   pip install -r requirements.txt
   ```
4. Start Command (Render liest das Procfile automatisch):
   ```
   web: gunicorn "ics_service:create_app()"
   ```
5. Environment Variables im Render Dashboard eintragen (wie in `.env`).

## Nutzung durch Kilchenmann
Nach Deployment stehen die ICS-Links bereit:
```
https://<render-app-name>.onrender.com/ics/biel_seminar.ics
https://<render-app-name>.onrender.com/ics/bern_gruppen.ics
```
Diese können in Outlook, Google Calendar oder Apple Calendar abonniert werden.
