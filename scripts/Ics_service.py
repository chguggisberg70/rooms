#!/usr/bin/env python3
"""
BFH ROOMS → Konsolidierter ICS-Feed für Gruppen/Standorte

Dieses Skript erzeugt ICS-Feeds aus der 3V ROOMS API.
Es unterstützt sowohl den Betrieb als WSGI-App (z. B. via gunicorn/uwsgi) als auch
als CLI-Tool zum direkten Export einzelner Gruppen in eine .ics-Datei.
"""
from __future__ import annotations

import os
import sys
import json
import hashlib
import time
import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

import requests
from flask import Flask, Response, abort

# -----------------------------
# Konfiguration
# -----------------------------
ROOMS_BASE_URL = os.getenv("ROOMS_BASE_URL")
IDP_TOKEN_URL = os.getenv("IDP_TOKEN_URL")
IDP_CLIENT_ID = os.getenv("IDP_CLIENT_ID")
IDP_CLIENT_SECRET = os.getenv("IDP_CLIENT_SECRET")
ROOMS_APIKEY_PIN = os.getenv("ROOMS_APIKEY_PIN")

USE_LEGACY_APIKEY = os.getenv("USE_LEGACY_APIKEY", "false").lower() == "true"
LEGACY_APIKEY = os.getenv("LEGACY_APIKEY")

USE_CAMEL = os.getenv("X_JSON_CAMEL", "true").lower() == "true"
TIMEZONE_LOCATION_ID = os.getenv("X_TIMEZONE_LOCATION_ID")

ICS_PROD_ID = os.getenv("ICS_PROD_ID", "-//BFH//3V-ROOMS ICS//DE")

_raw_group_map = os.getenv("GROUP_MAP_JSON", "{}")
try:
    GROUP_MAP = json.loads(_raw_group_map) if _raw_group_map.strip() else {}
    if not isinstance(GROUP_MAP, dict):
        raise ValueError("GROUP_MAP_JSON muss ein JSON-Objekt (Mapping) sein")
except Exception as e:
    raise RuntimeError(
        f'Fehler in GROUP_MAP_JSON: {e}. Beispiel: {{"biel_seminar":[101,102]}}'
    )

DAYS_PAST = int(os.getenv("DAYS_PAST", "1"))
DAYS_FUTURE = int(os.getenv("DAYS_FUTURE", "30"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))

app = Flask(__name__)

# -----------------------------
# Hilfsfunktionen
# -----------------------------


def rfc3339_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ics_escape(text: str) -> str:
    return (
        (text or "")
        .replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\n", "\\n")
    )


def make_uid(*parts: str) -> str:
    h = hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()
    return f"bfh-rooms-{h[:24]}@3vrooms"


def bearer_token() -> Optional[str]:
    if not (IDP_CLIENT_ID and ROOMS_APIKEY_PIN and IDP_TOKEN_URL):
        return None
    data = {
        "grant_type": "apikey",
        "scope": "rooms_api",
        "client_id": IDP_CLIENT_ID,
        "apikey": ROOMS_APIKEY_PIN,
    }
    if IDP_CLIENT_SECRET:
        data["client_secret"] = IDP_CLIENT_SECRET
    resp = requests.post(IDP_TOKEN_URL, data=data, timeout=20)
    resp.raise_for_status()
    return resp.json().get("access_token")


def auth_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {"Accept": "application/json"}
    if USE_CAMEL:
        headers["X-JsonPropertyFormat"] = "camel"
    if TIMEZONE_LOCATION_ID:
        headers["X-timezone-locationId"] = TIMEZONE_LOCATION_ID
        headers.setdefault("X-Timezone-LocationId", TIMEZONE_LOCATION_ID)

    if USE_LEGACY_APIKEY:
        if not LEGACY_APIKEY:
            raise RuntimeError("LEGACY_APIKEY fehlt")
        headers["APIKEY"] = LEGACY_APIKEY
        return headers

    token = bearer_token()
    if not token:
        raise RuntimeError("IDP-Token nicht konfiguriert und USE_LEGACY_APIKEY=false")
    headers["Authorization"] = f"Bearer {token}"
    return headers


# -----------------------------
# API-Aufruf → Buchungen
# -----------------------------


def fetch_bookings(
    resource_ids: List[int], date_from: datetime, date_to: datetime
) -> List[Dict[str, Any]]:
    if not ROOMS_BASE_URL:
        raise RuntimeError("ROOMS_BASE_URL ist nicht gesetzt.")

    headers = auth_headers()

    try:
        url = f"{ROOMS_BASE_URL}/api/v1.0/bookings"
        params = {
            "from": date_from.astimezone(timezone.utc).isoformat(),
            "to": date_to.astimezone(timezone.utc).isoformat(),
            "resourceIds": ",".join(str(x) for x in resource_ids),
        }
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            return normalize_bookings(r.json())
    except Exception:
        pass

    url = f"{ROOMS_BASE_URL}/api/v1.0/bookings/search"
    body = {
        "from": date_from.astimezone(timezone.utc).isoformat(),
        "to": date_to.astimezone(timezone.utc).isoformat(),
        "resourceIds": resource_ids,
    }
    r = requests.post(
        url,
        headers={**headers, "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    r.raise_for_status()
    return normalize_bookings(r.json())


def normalize_bookings(payload: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    if isinstance(payload, dict) and "items" in payload:
        data = payload["items"]
    elif isinstance(payload, list):
        data = payload
    else:
        data = []

    for b in data:
        bid = str(b.get("id") or b.get("bookingId") or b.get("guid") or "")
        title = b.get("title") or b.get("subject") or "Buchung"
        start = b.get("startUtc") or b.get("start") or b.get("startTime")
        end = b.get("endUtc") or b.get("end") or b.get("endTime")
        roomName = (
            (b.get("resource") or {}).get("name")
            or b.get("resourceName")
            or (b.get("room") or {}).get("name")
            or "Raum"
        )
        roomId = (
            (b.get("resource") or {}).get("id")
            or b.get("resourceId")
            or (b.get("room") or {}).get("id")
        )
        organizer = b.get("organizer") or ""
        location = b.get("location") or roomName

        def parse_dt(x: Optional[str]) -> Optional[datetime]:
            if not x:
                return None
            try:
                return datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(
                    timezone.utc
                )
            except Exception:
                return None

        start_dt = parse_dt(start) or datetime.now(timezone.utc)
        end_dt = parse_dt(end) or (start_dt + timedelta(hours=1))

        items.append(
            {
                "id": bid,
                "title": title,
                "startUtc": start_dt,
                "endUtc": end_dt,
                "roomName": roomName,
                "roomId": roomId,
                "organizer": organizer,
                "location": location,
            }
        )
    return items


# -----------------------------
# ICS-Erzeugung
# -----------------------------


def bookings_to_ics(bookings: List[Dict[str, Any]], prod_id: str = ICS_PROD_ID) -> str:
    now = datetime.now(timezone.utc)
    lines = [
        "BEGIN:VCALENDAR",
        f"PRODID:{ics_escape(prod_id)}",
        "VERSION:2.0",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]
    for b in bookings:
        uid = make_uid(str(b.get("id", "")), str(b.get("roomId", "")))
        dtstamp = rfc3339_utc(now)
        dtstart = rfc3339_utc(b["startUtc"])
        dtend = rfc3339_utc(b["endUtc"])
        summary = ics_escape(f"{b['roomName']}: {b['title']}")
        location = ics_escape(b.get("location") or b.get("roomName") or "")
        desc = ics_escape(b.get("organizer") or "")
        lines += [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{dtstamp}",
            f"DTSTART:{dtstart}",
            f"DTEND:{dtend}",
            f"SUMMARY:{summary}",
            f"LOCATION:{location}",
            f"DESCRIPTION:{desc}",
            "END:VEVENT",
        ]
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


# -----------------------------
# Cache
# -----------------------------
_cache: Dict[str, Dict[str, Any]] = {}


def cache_get(key: str) -> Optional[str]:
    entry = _cache.get(key)
    if not entry:
        return None
    if time.time() - entry["ts"] > CACHE_TTL:
        return None
    return entry["data"]


def cache_set(key: str, data: str) -> None:
    _cache[key] = {"ts": time.time(), "data": data}


# -----------------------------
# Flask-Routen
# -----------------------------
@app.get("/healthz")
def health() -> Response:
    return Response("ok", mimetype="text/plain")


@app.get("/ics/<group>.ics")
def ics_group(group: str):
    group_key = group.lower()
    room_ids = GROUP_MAP.get(group_key)
    if not room_ids:
        abort(
            404,
            f"Gruppe '{group_key}' unbekannt oder ohne Raum-IDs. Prüfe GROUP_MAP_JSON.",
        )
    cache_key = f"ics::{group_key}"
    cached = cache_get(cache_key)
    if cached:
        return Response(cached, mimetype="text/calendar; charset=utf-8")

    now = datetime.now(timezone.utc)
    date_from = now - timedelta(days=DAYS_PAST)
    date_to = now + timedelta(days=DAYS_FUTURE)

    bookings = fetch_bookings(room_ids, date_from, date_to)
    bookings.sort(key=lambda x: (x["startUtc"], str(x.get("roomName"))))

    ics_text = bookings_to_ics(bookings)
    cache_set(cache_key, ics_text)
    return Response(ics_text, mimetype="text/calendar; charset=utf-8")


# -----------------------------
# Factory & CLI
# -----------------------------
def create_app() -> Flask:
    """Factory für WSGI-Server (gunicorn/uwsgi)."""
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BFH ROOMS → ICS-Feed Service")
    parser.add_argument(
        "--group", help="Gruppe aus GROUP_MAP_JSON; gibt ICS auf stdout aus"
    )
    parser.add_argument(
        "--from", dest="date_from", help="Startdatum ISO (z.B. 2025-08-01)"
    )
    parser.add_argument("--to", dest="date_to", help="Enddatum ISO (z.B. 2025-09-01)")
    parser.add_argument(
        "--no-run",
        action="store_true",
        help="Webserver NICHT starten (nur CLI nutzen)",
    )
    args = parser.parse_args()

    if args.group:
        now = datetime.now(timezone.utc)
        df = (
            datetime.fromisoformat(args.date_from)
            if args.date_from
            else (now - timedelta(days=DAYS_PAST))
        )
        dt = (
            datetime.fromisoformat(args.date_to)
            if args.date_to
            else (now + timedelta(days=DAYS_FUTURE))
        )
        group_key = args.group.lower()
        room_ids = GROUP_MAP.get(group_key)
        if not room_ids:
            print(
                f"Gruppe '{group_key}' unbekannt. Verfügbar: {', '.join(sorted(GROUP_MAP.keys()))}",
                file=sys.stderr,
            )
            sys.exit(2)
        try:
            bookings = fetch_bookings(room_ids, df, dt)
        except Exception as e:
            print(f"Fehler beim Laden der Buchungen: {e}", file=sys.stderr)
            sys.exit(3)
        ics_text = bookings_to_ics(bookings)
        sys.stdout.write(ics_text)
        sys.exit(0)

    if (
        os.getenv("DISABLE_FLASK_RUN", "").lower() in ("1", "true", "yes")
        or args.no_run
    ):
        sys.exit(0)

    port = int(os.getenv("PORT", "8000"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)

"""
Beispiel .env
--------------
ROOMS_BASE_URL=https://<eure-instanz>/Default
IDP_TOKEN_URL=https://<euer-idp>/connect/token
IDP_CLIENT_ID=rooms_api_client
ROOMS_APIKEY_PIN=123456

# Legacy (falls nötig)
# USE_LEGACY_APIKEY=true
# LEGACY_APIKEY=123456

X_JSON_CAMEL=true

GROUP_MAP_JSON={
  "biel_seminar": [101,102,103],
  "bern_gruppen": [2201,2202]
}

DAYS_PAST=1
DAYS_FUTURE=60
CACHE_TTL=300
PORT=8000
"""
