#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BFH Rooms → Google Calendar (direkter Push, anonym, ohne CSV)

- Scrape via Playwright (persistentes Profil: --user-data-dir), setzt Zeitraum automatisch (heute → +range-days), klickt "Finden", sammelt alle Seiten
- Extrahiert nur Raum / Von / Bis (anonym)
- Push in Google Calendar (Kalendername parametrierbar, default "Rooms_BFH_Kilchenmann")
- Ersetzt zuvor die eigenen zukünftigen Events im Lookahead-Fenster (sauberer Re-Sync)

Beispiel:
  python rooms_sync_google.py --range-days 7 --user-data-dir pw_profile --calendar "Rooms_BFH_Kilchenmann"

Empfohlen für Scheduler (stündlich):
  - .bat anlegen, die venv aktiviert und dieses Script startet
  - Aufgabenplaner: /SC HOURLY
"""

import argparse
import asyncio
import csv
import hashlib
import re
from datetime import datetime, date, time, timedelta
from io import StringIO
from pathlib import Path
from typing import List, Optional, Tuple, Set

import pandas as pd
from bs4 import BeautifulSoup
from dateutil import tz
from playwright.async_api import (
    async_playwright,
    Page,
    Frame,
    TimeoutError as PWTimeoutError,
)

# ---- Google API ----
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ------------------ Konfiguration ------------------
BASE = "https://bfh.book.3vrooms.app"
URL_DEFAULT = f"{BASE}/Default/"
URL_FIND = f"{BASE}/Default/Lists/Reservation/FindReservation"

DATE_FROM_SELECTORS = [
    "input[name*='Von']",
    "input[id*='Von']",
    "input[name*='Begin']",
    "input[id*='Begin']",
    "input[name*='DatumVon']",
    "input[id*='DatumVon']",
]
DATE_TO_SELECTORS = [
    "input[name*='Bis']",
    "input[id*='Bis']",
    "input[name*='Ende']",
    "input[id*='Ende']",
    "input[name*='DatumBis']",
    "input[id*='DatumBis']",
]
FIND_BUTTON_SELECTORS = [
    "input[type='submit'][value='Finden']",
    "button:has-text('Finden')",
    "input[value='Finden']",
    "button[title='Finden']",
]
TABLE_FALLBACKS = [
    "table#ReservationList",
    "table#MainContent_ReservationList",
    "table.reservation-list",
    "table.dataTable",
    "table:has(thead)",
    "table",
]
NEXT_SELECTORS = [
    "a:has-text('›')",
    "a:has-text('»')",
    "a[title='Nächste']",
    "button:has-text('Nächste')",
    "img[alt='Nächste']",
    "img[title='Nächste']",
]
WANTED_COLS = {
    "von",
    "bis",
    "organisator",
    "buchungs",
    "firma",
    "ressource",
    "titel",
    "raum",
}

LOCAL_TZ = tz.gettz("Europe/Zurich")
SCOPES = ["https://www.googleapis.com/auth/calendar"]
SOURCE_TAG = "bfh-rooms-sync"


# ------------------ Helpers ------------------
def compute_dates(
    begin: Optional[str], end: Optional[str], range_days: int
) -> Tuple[str, str]:
    if begin and end:
        return begin, end
    today = date.today()
    return today.strftime("%Y-%m-%d"), (today + timedelta(days=range_days)).strftime(
        "%Y-%m-%d"
    )


async def ensure_logged_in(page: Page, timeout_ms: int) -> None:
    await page.goto(URL_DEFAULT, wait_until="domcontentloaded")
    try:
        await page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 60000))
    except PWTimeoutError:
        pass
    await page.goto(URL_FIND, wait_until="domcontentloaded")
    try:
        await page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 60000))
    except PWTimeoutError:
        pass


async def find_result_frame(page: Page, timeout_ms: int) -> Frame:
    candidates: List[Frame] = [
        f for f in page.frames if "FindReservation" in (f.url or "")
    ]
    candidates += [f for f in page.frames if f not in candidates]
    for f in candidates:
        try:
            await f.wait_for_selector("table tbody tr", timeout=1500, state="visible")
            return f
        except Exception:
            continue
    return page.main_frame


async def fill_dates_and_click_find(container, begin: str, end: str):
    von_ok = bis_ok = btn_ok = False
    for sel in DATE_FROM_SELECTORS:
        try:
            el = await container.wait_for_selector(sel, timeout=800)
            await el.fill(begin)
            von_ok = True
            break
        except Exception:
            continue
    for sel in DATE_TO_SELECTORS:
        try:
            el = await container.wait_for_selector(sel, timeout=800)
            await el.fill(end)
            bis_ok = True
            break
        except Exception:
            continue
    for sel in FIND_BUTTON_SELECTORS:
        try:
            btn = await container.wait_for_selector(sel, timeout=800)
            await btn.click()
            btn_ok = True
            break
        except Exception:
            continue
    return von_ok, bis_ok, btn_ok


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")
    if len(df) > 0 and (df.iloc[0].tolist() == df.columns.tolist()):
        df = df.iloc[1:].reset_index(drop=True)
    bad = df.apply(lambda r: "einträgen" in " ".join(map(str, r)).lower(), axis=1)
    df = df[~bad].reset_index(drop=True)
    drop_cols = [c for c in df.columns if c.strip() in {"", "R"}]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


def pick_best_table_from_html(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    if not tables:
        raise SystemExit("Keine <table> im HTML gefunden.")
    scored = []
    for t in tables:
        ths = [th.get_text(strip=True).lower() for th in t.find_all("th")]
        score = sum(1 for h in ths if any(w in h for w in WANTED_COLS))
        cols = len(ths) or len(t.find_all("td"))
        rows = len(t.find_all("tr"))
        scored.append((score, cols, rows, t))
    scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    best = scored[0][3]
    dfs = pd.read_html(StringIO(str(best)), flavor="lxml")
    if not dfs:
        raise SystemExit("Ergebnistabelle konnte nicht geparst werden.")
    return _clean_df(dfs[0])


def _unique_rows_key(df: pd.DataFrame) -> str:
    return "|".join(df.astype(str).head(3).fillna("").agg("||".join, axis=1).tolist())


async def collect_all_pages(frame: Frame, timeout_ms: int) -> pd.DataFrame:
    all_dfs: List[pd.DataFrame] = []
    seen: Set[str] = set()

    html = await frame.content()
    df = pick_best_table_from_html(html)
    fp = _unique_rows_key(df)
    seen.add(fp)
    all_dfs.append(df)

    for _ in range(40):
        clicked = False
        for sel in NEXT_SELECTORS:
            try:
                btn = await frame.wait_for_selector(sel, timeout=800)
                await btn.click()
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            # numerische Seiten 2..n
            try:
                links = frame.locator("a")
                count = await links.count()
                progressed = False
                for i in range(count):
                    a = links.nth(i)
                    label = (await a.text_content() or "").strip()
                    if not label.isdigit():
                        continue
                    try:
                        await a.click()
                        progressed = True
                        break
                    except Exception:
                        continue
                if not progressed:
                    break
            except Exception:
                break

        try:
            await frame.wait_for_selector(
                "table tbody tr", timeout=min(timeout_ms, 8000)
            )
        except Exception:
            pass
        await asyncio.sleep(0.4)

        html = await frame.content()
        df2 = pick_best_table_from_html(html)
        fp2 = _unique_rows_key(df2)
        if fp2 in seen:
            break
        seen.add(fp2)
        all_dfs.append(df2)

    big = pd.concat(all_dfs, ignore_index=True)
    big = big.drop_duplicates().reset_index(drop=True)
    return big


def normalize_to_room_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verschiedene Spalten-Layouts robust auf (Raum, Von, Bis) mappen.
    Fall A: benannte Spalten
    Fall B: numerische Spalten wie in deinem CSV-Snapshot (1=Von, 2=Bis, 6=Raum)
    """
    cols = [str(c) for c in df.columns]
    lower = {c.lower(): c for c in cols}

    def find_col(keys):
        for k in keys:
            for got in lower:
                if k in got:
                    return lower[got]
        return None

    col_raum = find_col(["raum", "ressource", "resource", "location", "standort"])
    col_von = find_col(["von", "start", "beginn", "startzeit", "begin"])
    col_bis = find_col(["bis", "ende", "end", "endzeit"])

    # numerischer Fallback
    if not (col_raum and col_von and col_bis) and all(
        c.isdigit() or c == "\ufeff0" for c in cols
    ):
        col_von = "1" if "1" in df.columns else list(df.columns)[1]
        col_bis = "2" if "2" in df.columns else list(df.columns)[2]
        col_raum = "6" if "6" in df.columns else list(df.columns)[6]

    # wenn immer noch nicht vorhanden: harter Abbruch mit Hinweis
    if not (col_raum and col_von and col_bis):
        raise SystemExit(f"Spalten nicht erkennbar. Habe: {df.columns.tolist()}")

    # Wochentag-Präfix „Mittwoch,“ entfernen
    weekday_prefix = re.compile(r"^[A-Za-zäöüÄÖÜß]+,\s*")

    def parse_dt(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        s = weekday_prefix.sub("", s)
        s = s.replace(",", " ")
        ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return ts

    out = pd.DataFrame(
        {
            "Raum": df[col_raum].astype(str).fillna(""),
            "Von": df[col_von].apply(parse_dt),
            "Bis": df[col_bis].apply(parse_dt),
        }
    )

    out = out[
        pd.notna(out["Von"]) & pd.notna(out["Bis"]) & (out["Bis"] > out["Von"])
    ].copy()
    # TZ setzen
    out["Von"] = out["Von"].apply(
        lambda x: (
            x.tz_localize(LOCAL_TZ) if x.tzinfo is None else x.tz_convert(LOCAL_TZ)
        )
    )
    out["Bis"] = out["Bis"].apply(
        lambda x: (
            x.tz_localize(LOCAL_TZ) if x.tzinfo is None else x.tz_convert(LOCAL_TZ)
        )
    )

    return out.sort_values(["Raum", "Von", "Bis"]).reset_index(drop=True)


# ------------------ Google Calendar ------------------
def load_gcal_service():
    creds = None
    token_path = Path("token.json")
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return build("calendar", "v3", credentials=creds)


def get_or_create_calendar(service, calendar_name: str) -> str:
    page_token = None
    while True:
        resp = (
            service.calendarList().list(pageToken=page_token, maxResults=250).execute()
        )
        for item in resp.get("items", []):
            if item.get("summary") == calendar_name:
                return item["id"]
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    created = (
        service.calendars()
        .insert(body={"summary": calendar_name, "timeZone": "Europe/Zurich"})
        .execute()
    )
    return created["id"]


def rfc3339_utc(ts: pd.Timestamp) -> str:
    return ts.tz_convert(tz.UTC).isoformat().replace("+00:00", "Z")


def fingerprint(row) -> str:
    base = f"{row['Raum']}|{row['Von'].isoformat()}|{row['Bis'].isoformat()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def delete_future_own_events(service, calendar_id: str, horizon_days: int):
    time_min = datetime.now(tz.UTC).isoformat()
    time_max = (datetime.now(tz.UTC) + timedelta(days=horizon_days)).isoformat()
    to_delete = []
    page_token = None
    while True:
        events = (
            service.events()
            .list(
                calendarId=calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                maxResults=2500,
                pageToken=page_token,
            )
            .execute()
        )
        for ev in events.get("items", []):
            props = ev.get("extendedProperties", {}).get("private", {})
            if props.get("source") == SOURCE_TAG:
                to_delete.append(ev["id"])
        page_token = events.get("nextPageToken")
        if not page_token:
            break

    if not to_delete:
        print("Keine eigenen zukünftigen Events zum Entfernen.")
        return
    print(f"Lösche {len(to_delete)} eigene zukünftige Events …")
    for eid in to_delete:
        service.events().delete(
            calendarId=calendar_id, eventId=eid, sendUpdates="none"
        ).execute()


def push_events(service, calendar_id: str, df: pd.DataFrame):
    inserted = 0
    for _, r in df.iterrows():
        fp = fingerprint(r)
        body = {
            "summary": "Belegt",  # anonym
            "location": str(r["Raum"]) or "",
            "description": "",
            "start": {"dateTime": rfc3339_utc(r["Von"]), "timeZone": "UTC"},
            "end": {"dateTime": rfc3339_utc(r["Bis"]), "timeZone": "UTC"},
            "transparency": "opaque",  # busy
            "visibility": "private",  # Details verbergen
            "extendedProperties": {"private": {"source": SOURCE_TAG, "fp": fp}},
        }
        service.events().insert(
            calendarId=calendar_id, body=body, sendUpdates="none"
        ).execute()
        inserted += 1
    print(f"Eingetragen: {inserted} Events")


# ------------------ Run ------------------
async def scrape_and_normalize(
    range_days: int, user_data_dir: str, timeout_ms: int, artifacts_dir: str
) -> pd.DataFrame:
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir, headless=True
        )
        page = await context.new_page()
        await ensure_logged_in(page, timeout_ms)

        begin, end = compute_dates(None, None, range_days)
        # Versuche im Hauptdokument und ggf. im Inhaltsframe
        von1, bis1, btn1 = await fill_dates_and_click_find(page, begin, end)
        frame = await find_result_frame(page, timeout_ms)
        von2, bis2, btn2 = await fill_dates_and_click_find(frame, begin, end)

        try:
            await frame.wait_for_selector(
                "table tbody tr", timeout=min(timeout_ms, 20000), state="visible"
            )
        except PWTimeoutError:
            pass

        Path(artifacts_dir).mkdir(parents=True, exist_ok=True)
        Path(f"{artifacts_dir}/frame_snapshot.html").write_text(
            await frame.content(), encoding="utf-8"
        )
        print(
            f"🗓️ Zeitraum: {begin} → {end} | Von={von1 or von2}, Bis={bis1 or bis2}, Finden={btn1 or btn2}"
        )

        # irgendeinen sichtbaren Table-Selector „berühren“, damit wir sicher sind, dass die Seite ready ist
        for sel in TABLE_FALLBACKS:
            try:
                await frame.wait_for_selector(sel, timeout=2000, state="visible")
                await frame.wait_for_selector(
                    "table tbody tr", timeout=2000, state="visible"
                )
                break
            except Exception:
                continue

        df_all = await collect_all_pages(frame, timeout_ms)
        await context.close()

    return normalize_to_room_times(df_all)


def load_args():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--range-days", type=int, default=7, help="Zeitraum ab heute (Default 7)"
    )
    ap.add_argument(
        "--user-data-dir",
        default="pw_profile",
        help="Persistentes Playwright-Profil (Ordner)",
    )
    ap.add_argument(
        "--calendar", default="Rooms_BFH_Kilchenmann", help="Google-Kalendername"
    )
    ap.add_argument(
        "--lookahead-days",
        type=int,
        default=90,
        help="Zukunftsfenster für Re-Sync in Google",
    )
    ap.add_argument("--timeout", type=int, default=30000, help="Timeout je Wait (ms)")
    ap.add_argument(
        "--artifacts-dir", default="artifacts_sync", help="Debug-HTML/Traces"
    )
    return ap.parse_args()


async def main():
    args = load_args()
    df = await scrape_and_normalize(
        args.range_days, args.user_data_dir, args.timeout, args.artifacts_dir
    )
    print(f"Scrape OK: {len(df)} Zeilen")
    svc = load_gcal_service()
    cal_id = get_or_create_calendar(svc, args.calendar)
    print(f"Kalender-ID: {cal_id}")
    delete_future_own_events(svc, cal_id, args.lookahead_days)
    push_events(svc, cal_id, df)
    print("✅ Sync fertig.")


if __name__ == "__main__":
    asyncio.run(main())
