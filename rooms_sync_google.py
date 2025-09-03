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
- Supports Playwright storage_state for SSO without interaction (--storage-state).
- Screenshots sind best-effort (safe_screenshot), damit es im Headless-Mode nicht hängt.
"""

import argparse
import asyncio
import hashlib
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

import pandas as pd
from dateutil import tz
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from tqdm import tqdm

# ------------------ CONFIG ------------------
class Config:
    BASE_URL = "https://bfh.book.3vrooms.app"
    FIND_URL = f"{BASE_URL}/Default/Lists/Reservation/FindReservation"

    ARTIFACTS_DIR = Path("artifacts_sync")
    USER_PROFILE_DIR = "pw_profile"
    CREDENTIALS_FILE = Path("credentials.json")
    TOKEN_FILE = Path("token.json")

    LOCAL_TIMEZONE_NAME = "Europe/Zurich"
    LOCAL_TIMEZONE = tz.gettz(LOCAL_TIMEZONE_NAME)
    SYNC_WINDOW_DAYS = 3
    GCAL_DELETE_HORIZON_DAYS = 8

    GCAL_SCOPES = ["https://www.googleapis.com/auth/calendar"]
    GCAL_SOURCE_TAG = "bfh-rooms-sync"
    DEFAULT_CALENDAR_NAME = "Rooms_BFH"
    GCAL_BATCH_CHUNK_SIZE = 50

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

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
)

# ------------------ HELPERS ------------------
def get_sync_window() -> Tuple[pd.Timestamp, pd.Timestamp]:
    today = pd.Timestamp(date.today(), tz=Config.LOCAL_TIMEZONE)
    start = today.normalize()
    end = (today + timedelta(days=Config.SYNC_WINDOW_DAYS)).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )
    return start, end

def format_for_sidepanel(ts: pd.Timestamp) -> str:
    return ts.strftime("%d.%m.%Y %H:%M")

async def safe_screenshot(page, path: Path, timeout_ms: int = 3000):
    try:
        await page.screenshot(path=path, timeout=timeout_ms)
        logging.info(f"Saved screenshot: {path.name}")
    except Exception as e:
        logging.warning(f"Skip screenshot ({path.name}): {e}")

# ------------------ SCRAPER ------------------
class BFHScraper:
    def __init__(self, timeout_ms: int, downloads_path: Path, headless: bool, storage_state: Optional[str] = None):
        self.timeout_ms = timeout_ms
        self.downloads_path = downloads_path
        self.headless = headless
        self.storage_state = storage_state
        self._pw = None
        self._browser = None
        self.context = None
        self.page = None

    async def __aenter__(self):
        self._pw = await async_playwright().start()

        if self.storage_state:
            logging.info(f"Using storage_state for auth: {self.storage_state}")
            if not os.path.isfile(self.storage_state):
                raise FileNotFoundError(f"storage_state.json nicht gefunden: {self.storage_state}")
            self._browser = await self._pw.chromium.launch(headless=self.headless)
            self.context = await self._browser.new_context(
                storage_state=self.storage_state,
                accept_downloads=True,
            )
        else:
            logging.info(f"Using persistent profile at: {Config.USER_PROFILE_DIR}")
            self.context = await self._pw.chromium.launch_persistent_context(
                user_data_dir=Config.USER_PROFILE_DIR,
                headless=self.headless,
                accept_downloads=True,
                slow_mo=50,
            )

        self.context.set_default_timeout(self.timeout_ms)
        self.context.set_default_navigation_timeout(self.timeout_ms)
        self.page = await self.context.new_page()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.context:
                await self.context.close()
            if self._browser:
                await self._browser.close()
        finally:
            if self._pw:
                await self._pw.stop()

    async def navigate_and_filter(self, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> bool:
        logging.info(f"Navigating to {Config.FIND_URL}...")
        await self.page.goto(Config.FIND_URL, wait_until="domcontentloaded", timeout=90000)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeoutError:
            logging.warning("Network idle timeout reached on initial load, continuing anyway.")

        logging.info("Setting side panel filters...")
        await safe_screenshot(self.page, Config.ARTIFACTS_DIR / "debug_before_check.png", timeout_ms=3000)

        try:
            await self.page.locator("#ReservationFilter_ManualDateTimeSelection").check(timeout=5000)
            await self.page.evaluate(
                f"document.querySelector('#ReservationFilter_Beginn').value = '{format_for_sidepanel(start_ts)}'"
            )
            await self.page.evaluate(
                f"document.querySelector('#ReservationFilter_Ende').value = '{format_for_sidepanel(end_ts)}'"
            )
            find_button = self.page.locator(",".join(Config.FIND_BUTTON_SELECTORS)).first
            if await find_button.is_visible():
                await find_button.click()
                logging.info("'Finden' button clicked.")
                try:
                    await self.page.wait_for_load_state("networkidle", timeout=30000)
                    logging.info("Search results appear to have loaded (network is idle).")
                except PWTimeoutError:
                    logging.warning("Timeout waiting for network idle after search.")
                return True
        except PWTimeoutError as e:
            logging.error("Checkbox for manual date selection not found!")
            await safe_screenshot(self.page, Config.ARTIFACTS_DIR / "debug_on_failure.png", timeout_ms=3000)
            raise e

        logging.warning("Could not find 'Finden' button, trying form submit.")
        await self.page.evaluate("document.querySelector('#sidePanelForm').submit()")
        return True

    # ... hier folgen noch get_csv_export, scrape_grid_fallback usw. (unverändert wie vorher) ...

# ------------------ MAIN ------------------
async def main(args: argparse.Namespace):
    Config.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    downloads_dir = Path(args.downloads) if args.downloads else (Path.home() / "Downloads")

    start_win, end_win = get_sync_window()
    logging.info(f"🗓️ Syncing for period: {start_win.strftime('%d.%m.%Y')} to {end_win.strftime('%d.%m.%Y')}")

    # hier dann: Scraper starten, CSV laden, normalisieren, mit GCal syncen

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync BFH room reservations to Google Calendar.")
    parser.add_argument("--timeout", type=int, default=180000, help="Timeout for browser operations in ms.")
    parser.add_argument("--calendar", default=Config.DEFAULT_CALENDAR_NAME, help="Base name for the Google Calendar.")
    parser.add_argument("--downloads", help="Override path to the browser's downloads folder.")
    parser.add_argument("--split-by", choices=["none", "standort", "gebaeude"], default="none")
    parser.add_argument("--html", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--storage-state", dest="storage_state", help="Pfad zu Playwright storage_state.json")
    args = parser.parse_args()
    asyncio.run(main(args))
