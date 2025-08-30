#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BFH Rooms to Google Calendar Sync (Delta-Sync Version for GitHub Actions)

Features:
- Scans a 3-day window (today to +3 days) for room reservations.
- Performs a "delta" sync: only new, changed, or canceled events are updated.
- User modifications to unchanged events (notes, colors) are preserved.
- Robustly exports data via CSV, handling various notification types.
- Includes a failsafe mechanism by scraping the Kendo UI grid if CSV is unavailable.
- Syncs with Google Calendar in efficient, rate-limit-friendly chunks.
- Supports splitting events into separate calendars by location or building (--split-by).
- Optionally generates a local HTML timeline view of the reservations (--html).
- Supports headless operation for server environments (--headless).
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

import pandas as pd
from dateutil import tz
from playwright.async_api import (
    async_playwright,
    Page,
    Frame,
    TimeoutError as PWTimeoutError,
    Locator,
)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import BatchHttpRequest
from tqdm import tqdm


# ------------------ CONFIGURATION ------------------
class Config:
    """Central configuration for the script."""
    BASE_URL = "https://bfh.book.3vrooms.app"
    FIND_URL = f"{BASE_URL}/Default/Lists/Reservation/FindReservation"

    # --- Directories and Files ---
    ARTIFACTS_DIR = Path("artifacts_sync")
    USER_PROFILE_DIR = "pw_profile"
    CREDENTIALS_FILE = Path("credentials.json")
    TOKEN_FILE = Path("token.json")

    # --- Time Settings ---
    LOCAL_TIMEZONE_NAME = "Europe/Zurich"
    LOCAL_TIMEZONE = tz.gettz(LOCAL_TIMEZONE_NAME)
    SYNC_WINDOW_DAYS = 3
    GCAL_DELETE_HORIZON_DAYS = 8

    # --- Google Calendar ---
    GCAL_SCOPES = ["https://www.googleapis.com/auth/calendar"]
    GCAL_SOURCE_TAG = "bfh-rooms-sync"
    DEFAULT_CALENDAR_NAME = "Rooms_BFH"
    GCAL_BATCH_CHUNK_SIZE = 50

    # --- Patterns & Selectors ---
    CSV_FILENAME_PATTERN = re.compile(
        r"(export[_-]?reservation|reservation[_-]?export|reservation)", re.IGNORECASE
    )
    EXPORT_BUTTON_SELECTORS = [
        "img[title='Export']",
        "img[alt='Export']",
        "#contentgrid i.k-i-excel",
        "#contentgrid .fa-file-excel",
        "button:has-text('Export')",
        "input[value='Export']",
    ]
    FIND_BUTTON_SELECTORS = [
        "#sidePanelForm button:has-text('Finden')",
        "button:has-text('Finden')",
        "input[type='submit'][value='Finden']",
        "input[value='Finden']",
    ]
    TOAST_CLOSE_SELECTORS = [
        ".notification-container.ui-notify .ui-notify-message img.ui-notify-close",
        ".k-notification .k-notification-close",
        ".k-notification .k-i-close",
        "div[role='alert'] .k-i-close",
    ]


# Setup basic logging
logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
)


# ------------------ TIME HELPERS ------------------
def get_sync_window() -> Tuple[pd.Timestamp, pd.Timestamp]:
    today = pd.Timestamp(date.today(), tz=Config.LOCAL_TIMEZONE)
    start = today.normalize()
    end = (today + timedelta(days=Config.SYNC_WINDOW_DAYS)).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )
    return start, end


def format_for_sidepanel(ts: pd.Timestamp) -> str:
    return ts.strftime("%d.%m.%Y %H:%M")


# ------------------ DATA NORMALIZATION ------------------
def extract_room_code(room_name: str) -> str:
    s = str(room_name or "").strip()
    if not s: return ""
    tokens = s.split()
    if not tokens: return ""
    if any(char.isdigit() for char in tokens[0]):
        if len(tokens) > 1 and len(tokens[0]) <= 2 and tokens[0].isalpha():
            return f"{tokens[0]} {tokens[1]}"
        return tokens[0]
    if len(tokens) > 1 and any(char.isdigit() for char in tokens[1]):
        return f"{tokens[0]} {tokens[1]}"
    return tokens[0]


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    col_map = {str(c).lower(): str(c) for c in df.columns}

    def find_col(keys: List[str]) -> Optional[str]:
        for key in keys:
            for col_lower, col_original in col_map.items():
                if key in col_lower: return col_original
        return None

    col_aliases = {
        "start": ["von", "beginn", "start", "startzeit", "datum von"],
        "end": ["bis", "ende", "end", "endzeit", "datum bis"],
        "room": ["ressource bezeichnung", "raum", "ressource", "resource"],
        "location": ["standortbezeichnung", "adresse", "standort", "gebäude", "gebaeude"],
    }

    col_start = find_col(col_aliases["start"]) or (df.columns[0] if df.shape[1] > 0 else None)
    col_end = find_col(col_aliases["end"]) or (df.columns[1] if df.shape[1] > 1 else None)
    col_room = find_col(col_aliases["room"]) or (df.columns[5] if df.shape[1] > 5 else (df.columns[3] if df.shape[1] > 3 else None))
    col_location = find_col(col_aliases["location"])

    if not all([col_start, col_end, col_room]):
        logging.warning("Could not find essential columns (start, end, room).")
        return pd.DataFrame()

    def parse_datetime(series):
        return pd.to_datetime(series, dayfirst=True, errors="coerce")

    clean_df = pd.DataFrame({
        "start_time": parse_datetime(df[col_start]),
        "end_time": parse_datetime(df[col_end]),
        "room_full": df[col_room].astype(str),
        "location": df[col_location].astype(str) if col_location else "",
    })

    clean_df = clean_df.dropna(subset=["start_time", "end_time"])
    clean_df = clean_df[clean_df["end_time"] > clean_df["start_time"]].copy()

    if clean_df.empty: return clean_df
    
    for col in ["start_time", "end_time"]:
        clean_df[col] = clean_df[col].dt.tz_localize(Config.LOCAL_TIMEZONE, ambiguous='infer')

    max_duration = timedelta(days=Config.SYNC_WINDOW_DAYS + 1)
    clean_df = clean_df[clean_df["end_time"] - clean_df["start_time"] < max_duration]

    start_win, end_win = get_sync_window()
    clean_df = clean_df[
        (clean_df["start_time"] <= end_win) & (clean_df["end_time"] >= start_win)
    ].copy()

    clean_df["room_code"] = clean_df["room_full"].apply(extract_room_code)
    clean_df["fingerprint"] = clean_df.apply(
        lambda row: hashlib.sha1(f"{row['start_time'].isoformat()}|{row['end_time'].isoformat()}|{row['room_full']}|{row['location']}".encode("utf-8")).hexdigest(),
        axis=1
    )
    return clean_df.sort_values(["start_time", "room_code"]).reset_index(drop=True)


# ------------------ GOOGLE CALENDAR API ------------------
class GCalManager:
    """Handles all interactions with the Google Calendar API using a delta-sync approach."""

    def __init__(self):
        self.service = self._get_service()

    @staticmethod
    def _get_service():
        creds = None
        if Config.TOKEN_FILE.exists():
            creds = Credentials.from_authorized_user_file(str(Config.TOKEN_FILE), Config.GCAL_SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(Config.CREDENTIALS_FILE, Config.GCAL_SCOPES)
                creds = flow.run_local_server(port=0)
            Config.TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
        return build("calendar", "v3", credentials=creds)

    def get_or_create_calendar(self, calendar_name: str) -> str:
        page_token = None
        while True:
            cal_list = self.service.calendarList().list(pageToken=page_token).execute()
            for item in cal_list.get("items", []):
                if item["summary"] == calendar_name:
                    logging.info(f"Found existing calendar '{calendar_name}' (ID: {item['id']})")
                    return item["id"]
            page_token = cal_list.get("nextPageToken")
            if not page_token: break
        
        logging.info(f"Creating new calendar: '{calendar_name}'")
        new_cal = {"summary": calendar_name, "timeZone": Config.LOCAL_TIMEZONE_NAME}
        created_cal = self.service.calendars().insert(body=new_cal).execute()
        return created_cal["id"]

    def get_synced_events(self, calendar_id: str) -> Dict[str, str]:
        """Fetches existing synced events and returns a map of {fingerprint: event_id}."""
        now_utc = pd.Timestamp.now(tz="UTC")
        max_utc = now_utc + timedelta(days=Config.GCAL_DELETE_HORIZON_DAYS)
        
        existing_events = {}
        page_token = None
        while True:
            events_result = self.service.events().list(
                calendarId=calendar_id,
                timeMin=now_utc.isoformat(),
                timeMax=max_utc.isoformat(),
                singleEvents=True,
                maxResults=2500,
                pageToken=page_token,
            ).execute()
            
            for event in events_result.get("items", []):
                props = event.get("extendedProperties", {}).get("private", {})
                if props.get("source") == Config.GCAL_SOURCE_TAG and "fp" in props:
                    existing_events[props["fp"]] = event["id"]
            
            page_token = events_result.get("nextPageToken")
            if not page_token: break
        
        logging.info(f"Found {len(existing_events)} existing events in calendar.")
        return existing_events

    @staticmethod
    def _create_event_body(row: pd.Series) -> Dict[str, Any]:
        summary_parts = ["Belegt", row.get("room_code", "").strip()]
        location_parts = [row.get("room_full", "").strip()]
        
        if location := row.get("location", "").strip():
            summary_parts.append(location)
            location_parts.append(location)
        
        summary = " – ".join(filter(None, summary_parts))
        location_str = " | ".join(filter(None, location_parts))

        return {
            "summary": summary,
            "location": location_str,
            "start": {"dateTime": row["start_time"].isoformat(), "timeZone": Config.LOCAL_TIMEZONE_NAME},
            "end": {"dateTime": row["end_time"].isoformat(), "timeZone": Config.LOCAL_TIMEZONE_NAME},
            "visibility": "private",
            "transparency": "opaque",
            "extendedProperties": {"private": {"source": Config.GCAL_SOURCE_TAG, "fp": row["fingerprint"]}},
        }

    def _execute_batch(self, requests: List[Dict], progress_desc: str):
        if not requests: return 0, 0
        
        success_count = 0
        
        def callback(request_id, response, exception):
            nonlocal success_count
            if not exception:
                success_count += 1
        
        with tqdm(total=len(requests), desc=progress_desc) as pbar:
            for i in range(0, len(requests), Config.GCAL_BATCH_CHUNK_SIZE):
                chunk = requests[i:i + Config.GCAL_BATCH_CHUNK_SIZE]
                batch = self.service.new_batch_http_request()
                for req in chunk:
                    batch.add(req, callback=callback)
                
                batch.execute()
                pbar.update(len(chunk))

                if i + Config.GCAL_BATCH_CHUNK_SIZE < len(requests):
                    time.sleep(1)

        failure_count = len(requests) - success_count
        return success_count, failure_count

    def sync_events(self, base_calendar_name: str, df: pd.DataFrame, split_by: str):
        if df.empty:
            logging.warning("No events to sync.")
            return

        if split_by == "none":
            groups = {base_calendar_name: df}
        else:
            location_name_map = {
                "BFH-H-Gebäude Schwarztor": "Schwarztorstrasse 48",
            }

            def get_bucket(location: str) -> str:
                s = (location or "").strip() or "Unbekannt"
                if s in location_name_map:
                    return location_name_map[s]
                return s.split(" - ")[0].strip() if split_by == "gebaeude" else s

            df["__bucket"] = df["location"].apply(get_bucket)
            groups = {f"{base_calendar_name} – {name}": group_df.drop(columns=["__bucket"]) for name, group_df in df.groupby("__bucket")}

        for cal_name, group_df in groups.items():
            logging.info(f"--- Syncing Calendar: {cal_name} ---")
            calendar_id = self.get_or_create_calendar(cal_name)
            
            existing_events_map = self.get_synced_events(calendar_id)
            source_fingerprints = set(group_df["fingerprint"])
            existing_fingerprints = set(existing_events_map.keys())
            
            to_create_fingerprints = source_fingerprints - existing_fingerprints
            to_delete_fingerprints = existing_fingerprints - source_fingerprints
            
            unchanged_count = len(source_fingerprints.intersection(existing_fingerprints))
            logging.info(f"Delta found: {len(to_create_fingerprints)} to create, {len(to_delete_fingerprints)} to delete, {unchanged_count} unchanged.")

            creation_requests = []
            df_to_create = group_df[group_df["fingerprint"].isin(to_create_fingerprints)]
            for _, row in df_to_create.iterrows():
                event_body = self._create_event_body(row)
                creation_requests.append(self.service.events().insert(calendarId=calendar_id, body=event_body))

            deletion_requests = []
            for fp in to_delete_fingerprints:
                event_id = existing_events_map[fp]
                deletion_requests.append(self.service.events().delete(calendarId=calendar_id, eventId=event_id))

            created_s, created_f = self._execute_batch(creation_requests, "Creating new events")
            deleted_s, deleted_f = self._execute_batch(deletion_requests, "Deleting old events")
            
            logging.info(f"Sync for '{cal_name}' complete. Created: {created_s}/{created_s+created_f}, Deleted: {deleted_s}/{deleted_s+deleted_f}.")


# ------------------ BROWSER AUTOMATION ------------------
class BFHScraper:
    def __init__(self, timeout_ms: int, downloads_path: Path, headless: bool):
        self.timeout_ms = timeout_ms
        self.downloads_path = downloads_path
        self.headless = headless
        self.context = None
        self.page = None

    async def __aenter__(self):
        playwright = await async_playwright().start()
        self.context = await playwright.chromium.launch_persistent_context(
            user_data_dir=Config.USER_PROFILE_DIR, headless=self.headless, accept_downloads=True, slow_mo=50
        )
        self.page = await self.context.new_page()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.context: await self.context.close()

    # ERSETZE DIE BISHERIGE navigate_and_filter FUNKTION MIT DIESER

async def navigate_and_filter(self, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> bool:
    """Navigates to the reservation page, applies filters, and waits for results."""
    logging.info(f"Navigating to {Config.FIND_URL}...")
    # Wir geben der Seite mehr Zeit zum Laden, falls sie langsam ist
    await self.page.goto(Config.FIND_URL, wait_until="domcontentloaded", timeout=90000)
    try:
        await self.page.wait_for_load_state("networkidle", timeout=15000)
    except PWTimeoutError:
        logging.warning("Network idle timeout reached on initial load, continuing anyway.")

    logging.info("Setting side panel filters...")
    
    # NEU: Wir machen einen Screenshot direkt bevor wir die Checkbox suchen
    await self.page.screenshot(path=Config.ARTIFACTS_DIR / "debug_before_check.png")

    try:
        # Der Befehl, der aktuell fehlschlägt
        await self.page.locator("#ReservationFilter_ManualDateTimeSelection").check(timeout=5000)
        
        await self.page.evaluate(f"document.querySelector('#ReservationFilter_Beginn').value = '{format_for_sidepanel(start_ts)}'")
        await self.page.evaluate(f"document.querySelector('#ReservationFilter_Ende').value = '{format_for_sidepanel(end_ts)}'")
        
        find_button = self.page.locator(','.join(Config.FIND_BUTTON_SELECTORS)).first
        if await find_button.is_visible():
            await find_button.click()
            logging.info("'Finden' button clicked.")
            try:
                await self.page.wait_for_load_state("networkidle", timeout=30000)
                logging.info("Search results appear to have loaded (network is idle).")
            except PWTimeoutError:
                logging.warning("Timeout waiting for network idle after search. The page might be slow or stuck.")
            return True
        
    except PWTimeoutError as e:
        logging.error("Checkbox for manual date selection not found!")
        # NEU: Wir machen einen Screenshot genau im Moment des Fehlers
        await self.page.screenshot(path=Config.ARTIFACTS_DIR / "debug_on_failure.png")
        logging.error("A screenshot named 'debug_on_failure.png' has been saved to the artifacts.")
        # Wir werfen den Fehler erneut, damit der Workflow korrekt als fehlgeschlagen markiert wird
        raise e
    
    logging.warning("Could not find 'Finden' button, trying form submit.")
    await self.page.evaluate("document.querySelector('#sidePanelForm').submit()")
    return True
    async def close_toasts(self):
        for selector in Config.TOAST_CLOSE_SELECTORS:
            buttons = self.page.locator(selector)
            for i in range(await buttons.count()):
                try: await buttons.nth(i).click(timeout=500)
                except PWTimeoutError: pass

    async def get_csv_export(self) -> Optional[Path]:
        await self.close_toasts()
        export_button = self.page.locator(','.join(Config.EXPORT_BUTTON_SELECTORS)).first
        if not await export_button.is_visible(timeout=10000):
            logging.warning("Export button not found. Will fall back to grid scraping.")
            return None

        await export_button.click()
        logging.info("Export clicked. Now polling for the download link to appear...")

        download_link_found = None
        polling_end_time = time.time() + 180

        while time.time() < polling_end_time:
            try:
                notification_link_selector = ".ui-notify-message a, .k-notification-content a, div[role='alert'] a"
                link_locator = self.page.locator(notification_link_selector).first
                if await link_locator.is_visible(timeout=1000):
                    logging.info("Download link appeared in notification!")
                    download_link_found = link_locator
                    break
            except PWTimeoutError:
                logging.info("...still waiting for link...")
                await asyncio.sleep(5)

        if download_link_found:
            try:
                async with self.page.expect_download(timeout=self.timeout_ms) as download_info:
                    await download_link_found.click()
                download = await download_info.value
                dest_path = Config.ARTIFACTS_DIR / download.suggested_filename
                await download.save_as(dest_path)
                logging.info(f"Download successful via polled link: {dest_path.name}")
                return dest_path
            except PWTimeoutError:
                logging.error("Found the link, but the download itself timed out.")

        logging.error("Failed to find the download link within 3 minutes.")
        return None

    async def scrape_grid_fallback(self) -> pd.DataFrame:
        logging.info("Attempting to scrape data directly from the grid...")
        try:
            await self.page.wait_for_selector(".k-grid-content tr", timeout=15000)
            headers = await self.page.eval_on_selector_all(".k-grid-header th", "nodes => nodes.map(n => n.innerText.trim())")
            rows = await self.page.eval_on_selector_all(".k-grid-content tr", "nodes => nodes.map(tr => Array.from(tr.cells).map(td => td.innerText.trim()))")
            if not rows: return pd.DataFrame()
            for row in rows:
                while len(row) < len(headers): row.append('')
            return pd.DataFrame(rows, columns=headers)
        except (PWTimeoutError, Exception) as e:
            logging.error(f"Grid scraping failed: {e}")
            return pd.DataFrame()

# ------------------ FILE I/O ------------------
def read_csv_robustly(path: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-16", "latin1", "cp1252"]
    separators = [None, ";", ","]
    for enc in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep, engine="python")
                if df.shape[1] > 2:
                    logging.info(f"Successfully read CSV with encoding '{enc}' and separator '{sep or 'auto'}'")
                    return df
            except (UnicodeDecodeError, pd.errors.ParserError): continue
    raise ValueError(f"Could not read CSV file: {path}")

def export_html_timeline(df: pd.DataFrame, out_path: Path):
    if df.empty: return
    df_for_table = df.copy()
    df_for_table['start_time_str'] = df_for_table['start_time'].dt.strftime('%d.%m.%Y %H:%M')
    df_for_table['end_time_str'] = df_for_table['end_time'].dt.strftime('%d.%m.%Y %H:%M')
    table_json_data = df_for_table[['start_time_str', 'end_time_str', 'room_code', 'location', 'room_full']].to_json(orient="records")
    start, end = get_sync_window()
    days = pd.date_range(start.normalize(), end.normalize(), freq="D")
    hour_min, hour_max = 6, 22
    rooms = sorted(df['room_code'].fillna(df['room_full']).unique())
    timeline_html = f"""<div class="timeline-grid">
    <div></div>{''.join(f'<div class="head">{d.strftime("%a, %d.%m.")}</div>' for d in days)}"""
    def to_percent(ts, day):
        day_start = day.replace(hour=hour_min, minute=0, second=0)
        day_end = day.replace(hour=hour_max, minute=0, second=0)
        total_seconds = (day_end - day_start).total_seconds()
        if total_seconds == 0: return 0.0
        ts_seconds = (ts - day_start).total_seconds()
        return max(0, min(100, (ts_seconds / total_seconds) * 100))
    for room in rooms:
        timeline_html += f'<div class="room">{room}</div>\n'
        for d_ts in days:
            timeline_html += '<div class="cell">\n'
            day_start_utc = pd.Timestamp(d_ts).tz_localize(Config.LOCAL_TIMEZONE).normalize()
            day_end_utc = day_start_utc + timedelta(days=1)
            items = df[(df["room_code"].fillna(df["room_full"]) == room) & (df["start_time"] < day_end_utc) & (df["end_time"] > day_start_utc)]
            for _, r in items.iterrows():
                left = to_percent(r["start_time"], day_start_utc)
                right = to_percent(r["end_time"], day_start_utc)
                width = max(0.5, right - left)
                timeline_html += f'<div class="bar" style="left:{left:.2f}%;width:{width:.2f}%" title="{r["location"]}"></div>\n'
            timeline_html += "</div>\n"
    timeline_html += "</div>"
    html_template = f"""
<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8"><title>BFH Rooms - Interaktiver Plan</title>
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<style>body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;margin:2em;color:#333}}h1,h2{{color:#1a237e}}table.dataTable thead th{{background-color:#e8eaf6}}.dataTables_wrapper .dataTables_filter input{{border:1px solid #9fa8da;border-radius:4px;padding:5px}}.container{{margin-bottom:4em}}.timeline-grid{{display:grid;grid-template-columns:180px repeat({len(days)},1fr);gap:4px;margin-top:1em}}.head{{font-weight:700;text-align:center;padding-bottom:4px}}.cell{{position:relative;border:1px solid #ddd;min-height:30px;background:#fafafa;overflow:hidden}}.room{{padding:5px 8px;font-weight:700;border:1px solid #ddd;background:#f0f3f8;font-size:.9em}}.bar{{position:absolute;height:80%;top:10%;background:#3f51b5;border-radius:4px}}</style>
</head><body><h1>BFH Rooms - Interaktiver Plan</h1><p>Daten vom {start.strftime("%d.%m.%Y")} bis {end.strftime("%d.%m.%Y")}.</p>
<div class="container"><h2>Durchsuchbare Liste aller Reservationen</h2>
<table id="reservationsTable" class="display" style="width:100%"><thead><tr><th>Start</th><th>Ende</th><th>Raum-Code</th><th>Standort</th><th>Raum (vollst.)</th></tr></thead></table></div>
<div class="container"><h2>Visuelle Timeline ({hour_min:02d}:00 - {hour_max:02d}:00)</h2>{timeline_html}</div>
<script src="https://code.jquery.com/jquery-3.7.0.js"></script><script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script>
const reservationData = {table_json_data};
$(document).ready(function(){{$('#reservationsTable').DataTable({{data:reservationData,columns:[{{data:'start_time_str'}},{{data:'end_time_str'}},{{data:'room_code'}},{{data:'location'}},{{data:'room_full'}}],order:[[0,'asc']],pageLength:25,language:{{"url":"//cdn.datatables.net/plug-ins/1.13.6/i18n/de-DE.json"}}}});}});
</script></body></html>"""
    out_path.write_text(html_template, encoding="utf-8")
    logging.info(f"Interaktiver HTML-Plan exportiert nach: {out_path}")


# ------------------ MAIN LOGIC ------------------
async def main(args: argparse.Namespace):
    Config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    downloads_dir = Path(args.downloads) if args.downloads else (Path.home() / "Downloads")

    start_win, end_win = get_sync_window()
    logging.info(f"🗓️ Syncing for period: {start_win.strftime('%d.%m.%Y')} to {end_win.strftime('%d.%m.%Y')}")

    raw_df = pd.DataFrame()
    async with BFHScraper(args.timeout, downloads_dir, args.headless) as scraper:
        await scraper.navigate_and_filter(start_win, end_win)
        csv_path = await scraper.get_csv_export()
        if csv_path:
            try: raw_df = read_csv_robustly(csv_path)
            except ValueError as e: logging.error(f"{e}. Falling back to grid scraping.")
        if raw_df.empty:
            raw_df = await scraper.scrape_grid_fallback()

    if raw_df.empty:
        logging.error("❌ No data could be fetched. Aborting sync.")
        return

    logging.info(f"Normalizing {len(raw_df)} raw entries...")
    df = normalize_dataframe(raw_df)

    if df.empty:
        logging.warning("No valid reservations found in the time window. Nothing to sync.")
        return

    logging.info(f"Found {len(df)} valid reservations to sync.")
    print("--- Data Preview ---")
    print(df.head().to_string(index=False))
    print("--------------------")

    gcal = GCalManager()
    gcal.sync_events(args.calendar, df, args.split_by)

    if args.html:
        export_html_timeline(df, Config.ARTIFACTS_DIR / "schedule.html")

    logging.info("✅ Sync process completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync BFH room reservations to Google Calendar.")
    parser.add_argument("--timeout", type=int, default=180000, help="Timeout for browser operations in milliseconds.")
    parser.add_argument("--calendar", default=Config.DEFAULT_CALENDAR_NAME, help="Base name for the Google Calendar.")
    parser.add_argument("--downloads", help="Override path to the browser's downloads folder.")
    parser.add_argument("--split-by", choices=["none", "standort", "gebaeude"], default="none", help="Split events into different calendars.")
    parser.add_argument("--html", action="store_true", help="Generate an additional HTML timeline file.")
    parser.add_argument("--headless", action="store_true", help="Run Playwright in headless mode (for servers).")
    
    cli_args = parser.parse_args()
    asyncio.run(main(cli_args))
