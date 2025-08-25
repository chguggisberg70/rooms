# -*- coding: utf-8 -*-
"""
rooms_sync_google.py

Optional:
- --split-by standort / gebaeude
- --no-chunk um Tages-Splitting 06-22 abzuschalten
Zusaetzlich: HTML-Tafel (artifacts_sync/schedule.html)

Start (PowerShell):
  $env:ROOMS_USER="dein@login"
  $env:ROOMS_PASS="deinpass"
  python rooms_sync_google.py --calendar "Rooms_BFH_Kilchenmann" --split-by standort --timeout 240000 --prefer-grid
"""

import argparse
import asyncio
import hashlib
import json
import os
import re
import time
import smtplib, ssl
from email.message import EmailMessage
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple, Optional, Dict

import pandas as pd
from dateutil import tz
from playwright.async_api import (
    async_playwright,
    Page,
    Frame,
    TimeoutError as PWTimeoutError,
)

# Google
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ------------------ Konfiguration ------------------
BASE = "https://bfh.book.3vrooms.app"
URL_FIND = f"{BASE}/Default/Lists/Reservation/FindReservation"

SCRIPT_DIR = Path(__file__).resolve().parent
USER_DATA_DIR = SCRIPT_DIR / "pw_profile"  # persistentes Profil (kein staendiges Login)
ARTIFACTS_DIR = SCRIPT_DIR / "artifacts_sync"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

ROOMS_CSV_PATTERN = re.compile(r"(export[_-]?reservation|reservation[_-]?export|reservation)", re.I)
LOCAL_TZ = tz.gettz("Europe/Zurich")
SCOPES = ["https://www.googleapis.com/auth/calendar"]
SOURCE_TAG = "bfh-rooms-sync"

# ------------------ Zeitraum ------------------

def compute_window_days_7() -> Tuple[pd.Timestamp, pd.Timestamp]:
    today = pd.Timestamp(date.today(), tz=LOCAL_TZ)
    start = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end = (today + pd.Timedelta(days=7)).replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end

# ------------------ CSV robust laden ------------------

def read_csv_smart(path: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-16", "utf-16-le", "cp1252", "latin1"]
    for enc in encodings:
        for header in [True, False]:
            try:
                if header:
                    df = pd.read_csv(path, sep=None, engine="python", encoding=enc)
                else:
                    df = pd.read_csv(path, sep=None, engine="python", encoding=enc, header=None)
                if df.shape[1] <= 1:
                    df = pd.read_csv(path, sep=";", encoding=enc, header=(0 if header else None))
                return df
            except Exception:
                pass
    raise RuntimeError(f"CSV konnte nicht gelesen werden: {path}")

# ------------------ Normalisierung ------------------

def extract_room_code(raum: str) -> str:
    s = str(raum or "").strip()
    if not s:
        return ""
    tokens = s.split()
    if not tokens:
        return ""
    if any(ch.isdigit() for ch in tokens[0]):
        code = tokens[0]
        if len(tokens) >= 2 and (len(code) <= 2 and code.isalpha()) and any(ch.isdigit() for ch in tokens[1]):
            return f"{tokens[0]} {tokens[1]}"
        return code
    if len(tokens) >= 2 and any(ch.isdigit() for ch in tokens[1]):
        return f"{tokens[0]} {tokens[1]}"
    return tokens[0]

def _guess_cols_by_content(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    # Spaltennamen normalisieren
    df = df.copy()
    df.columns = [str(c).replace("\u00A0", " ").replace("\u202F", " ").strip() for c in df.columns]

    sample = df.head(120).copy()

    def try_parse_dt(x):
        s = str(x).replace("\u00A0", " ").replace("\u202F", " ").strip()
        s = s.replace(" Uhr", "")
        s = re.sub(r"^[A-Za-zÄÖÜäöüß]+\s*,\s*", "", s)
        s = s.replace(",", " ")
        return pd.to_datetime(s, dayfirst=True, errors="coerce")

    # Datums-/Zeitspalten schaetzen (Schwelle entschaerft)
    dt_scores = {c: sample[c].apply(try_parse_dt).notna().mean() for c in sample.columns}
    dt_sorted = [c for c, sc in sorted(dt_scores.items(), key=lambda kv: kv[1], reverse=True) if sc >= 0.30]
    col_von = dt_sorted[0] if len(dt_sorted) >= 1 else None
    col_bis = dt_sorted[1] if len(dt_sorted) >= 2 else None

    # Raumspalte per Muster
    room_pat = re.compile(r"(^|\b)([A-ZÄÖÜ]{1,3}\s?\d{1,4}|\d{2,4}|[A-ZÄÖÜ]{1,2}\d{2,4})(\b|\s)")
    room_scores = {c: sample[c].astype(str).apply(lambda s: bool(room_pat.search(s))).mean() for c in sample.columns}
    cand_room = [c for c, sc in sorted(room_scores.items(), key=lambda kv: kv[1], reverse=True) if sc >= 0.30]
    col_raum = cand_room[0] if cand_room else (df.columns[6] if len(df.columns) > 6 else (df.columns[3] if len(df.columns) > 3 else None))

    # Standort/Adresse (heuristisch)
    addr_pat = re.compile(r"(strasse|str\.|platz|gasse|weg|allee|quai|ring|stras|platz)\b", re.I)
    site_scores = {c: sample[c].astype(str).apply(lambda s: (" - " in s) or bool(addr_pat.search(s))).mean() for c in sample.columns}
    cand_site = [c for c, sc in sorted(site_scores.items(), key=lambda kv: kv[1], reverse=True) if sc >= 0.30]
    col_site = cand_site[0] if cand_site else None
    return col_von, col_bis, col_raum, col_site

def normalize_to_room_times(df: pd.DataFrame) -> pd.DataFrame:
    # Spaltennamen reinigen (klein, NBSP raus)
    df = df.copy()
    df.columns = [str(c).replace("\u00A0", " ").replace("\u202F", " ").strip().lower() for c in df.columns]

    def try_parse_dt(x):
        s = str(x)
        s = s.replace("\u00A0", " ").replace("\u202F", " ").strip()
        s = re.sub(r"^[A-Za-zÄÖÜäöüß]+\s*,\s*", "", s)
        s = s.replace(" Uhr", "")
        s = s.replace(",", " ")
        s = re.sub(r"\s+", " ", s)
        return pd.to_datetime(s, dayfirst=True, errors="coerce")

    lower = {c: c for c in df.columns}

    def find_col(keys):
        for key in keys:
            for lc in lower:
                if key in lc:
                    return lower[lc]
        return None

    col_von  = find_col(["von", "beginn", "start"]) or None
    col_bis  = find_col(["bis", "ende", "end"]) or None
    col_raum = find_col(["ressource bezeichnung", "ressourcen id/bezeichnung", "raum", "ressource", "resource"]) or None
    col_site = find_col(["standortbezeichnung", "standort", "adresse", "strasse"]) or None

    if not (col_von and col_bis and col_raum):
        gv, gb, gr, gs = _guess_cols_by_content(df)
        col_von = col_von or (gv.lower() if gv else None)
        col_bis = col_bis or (gb.lower() if gb else None)
        col_raum = col_raum or (gr.lower() if gr else None)
        col_site = col_site or (gs.lower() if gs else None)

    cols = list(df.columns)
    if not col_von and len(cols) > 1:  col_von = cols[1]
    if not col_bis and len(cols) > 2:  col_bis = cols[2]
    if not col_raum and len(cols) > 6: col_raum = cols[6]
    if not col_site and len(cols) > 13: col_site = cols[13]

    if not (col_von and col_bis and col_raum):
        return pd.DataFrame(columns=["Von", "Bis", "Raum", "Standort", "Raumcode"])

    out = pd.DataFrame({
        "Von": df[col_von].apply(try_parse_dt),
        "Bis": df[col_bis].apply(try_parse_dt),
        "Raum": df[col_raum].astype(str),
        "Standort": df[col_site].astype(str) if col_site else "",
    })

    out = out[pd.notna(out["Von"]) & pd.notna(out["Bis"]) & (out["Bis"] > out["Von"])].copy()
    if out.empty:
        return out

    out["Von"] = out["Von"].apply(lambda x: x.tz_localize(LOCAL_TZ) if x.tzinfo is None else x.tz_convert(LOCAL_TZ))
    out["Bis"] = out["Bis"].apply(lambda x: x.tz_localize(LOCAL_TZ) if x.tzinfo is None else x.tz_convert(LOCAL_TZ))

    start, end = compute_window_days_7()
    inside = out[(out["Von"] <= end) & (out["Bis"] >= start)].copy()

    if inside.empty:
        try:
            dmin = out["Von"].min()
            dmax = out["Bis"].max()
            print(f"[NORM] Hinweis: Parsed date range = {dmin} .. {dmax} (keine Events im 7-Tage-Fenster)")
        except Exception:
            pass
        return inside

    inside["Raumcode"] = inside["Raum"].apply(extract_room_code)
    inside = inside.sort_values(["Von", "Raum"]).reset_index(drop=True)
    return inside

def chunk_to_days_6_22(df: pd.DataFrame) -> pd.DataFrame:
    """Schneidet Events ins 7-Tage-Fenster und zerteilt mehrtaegige in Tages-Scheiben 06:00-22:00."""
    if df.empty:
        return df
    start, end = compute_window_days_7()
    rows = []
    for _, r in df.iterrows():
        st = max(r["Von"], start)
        en = min(r["Bis"], end)
        if en <= st:
            continue
        cur_day = pd.Timestamp(st.year, st.month, st.day, 0, 0, 0, tzinfo=LOCAL_TZ)
        last_day = pd.Timestamp(en.year, en.month, en.day, 0, 0, 0, tzinfo=LOCAL_TZ)
        while cur_day <= last_day:
            d_start = pd.Timestamp(cur_day.year, cur_day.month, cur_day.day, 6, 0, 0, tzinfo=LOCAL_TZ)
            d_end   = pd.Timestamp(cur_day.year, cur_day.month, cur_day.day, 22, 0, 0, tzinfo=LOCAL_TZ)
            s = max(st, d_start)
            e = min(en, d_end)
            if e > s:
                nr = r.copy()
                nr["Von"] = s
                nr["Bis"] = e
                rows.append(nr)
            cur_day = cur_day + pd.Timedelta(days=1)
    if not rows:
        return df.iloc[0:0].copy()
    out = pd.DataFrame(rows)
    return out.sort_values(["Von","Raum"]).reset_index(drop=True)

# ------------------ HTML-Tafel ------------------

def export_html_timeline(df: pd.DataFrame, dest: Path) -> None:
    if df.empty:
        dest.write_text("<html><body>Keine Daten</body></html>", encoding="utf-8")
        print(f"[HTML] Tafel: {dest}")
        return
    start, end = compute_window_days_7()
    days = [start + pd.Timedelta(days=i) for i in range(7)]

    rooms = (
        df[["Raumcode","Raum"]]
        .assign(_key=df["Raumcode"].fillna("")+"|"+df["Raum"].fillna(""))
        .drop_duplicates(subset="_key")
    )
    room_order = rooms["Raumcode"].replace("", pd.NA).fillna(rooms["Raum"]).fillna("").tolist()

    by_room: Dict[str, List[pd.Series]] = {r: [] for r in room_order}
    for _, row in df.iterrows():
        rc = str(row["Raumcode"] or row["Raum"] or "")
        by_room.setdefault(rc, []).append(row)

    css = """
    <style>
      body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Arial,sans-serif;margin:16px}
      .grid{display:grid;grid-template-columns:200px repeat(7,1fr);gap:8px}
      .h{font-weight:600;background:#f4f6f8;padding:8px;border:1px solid #e5e7eb;border-radius:8px;text-align:center}
      .r{background:#fff;padding:8px;border:1px solid #e5e7eb;border-radius:8px}
      .cell{position:relative;height:60px;background:#fafafa;border:1px dashed #e5e7eb;border-radius:8px;overflow:hidden}
      .bar{position:absolute;left:0;right:0;height:22px;margin:2px;border-radius:6px;background:#7c3aed;opacity:.85;color:#fff;font-size:12px;line-height:22px;padding:0 6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    </style>
    """
    html = [
        "<html><head><meta charset='utf-8'>", css, "</head><body>",
        f"<h2>Belegungen {start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}</h2>",
        "<div class='grid'>",
        "<div class='h'>Raum</div>",
    ]
    for d in days:
        html.append(f"<div class='h'>{d.strftime('%a %d.%m')}</div>")
    for room in room_order:
        html.append(f"<div class='r'><div><b>{room}</b></div></div>")
        for d in days:
            html.append("<div class='cell'>")
            day_start = pd.Timestamp(d.year,d.month,d.day,6,0,0,tzinfo=LOCAL_TZ)
            day_end   = pd.Timestamp(d.year,d.month,d.day,22,0,0,tzinfo=LOCAL_TZ)
            total = (day_end-day_start).total_seconds()
            for r in by_room.get(room, []):
                st = r["Von"].tz_convert(LOCAL_TZ)
                en = r["Bis"].tz_convert(LOCAL_TZ)
                if st.date() > d.date() or en.date() < d.date():
                    continue
                s = max(st,day_start)
                e = min(en,day_end)
                if e <= s:
                    continue
                left = (s-day_start).total_seconds()/total*100.0
                width = (e-s).total_seconds()/total*100.0
                label = f"{s.strftime('%H:%M')} - {e.strftime('%H:%M')}"
                html.append(f"<div class='bar' style='left:{left:.2f}%;width:{width:.2f}%' title='{label}'>{label}</div>")
            html.append("</div>")
    html.append("</div></body></html>")
    dest.write_text("".join(html), encoding="utf-8")
    print(f"[HTML] Tafel: {dest}")

# ------------------ Google Calendar ------------------

def load_gcal_service():
    creds = None
    tp = SCRIPT_DIR / "token.json"
    if tp.exists():
        creds = Credentials.from_authorized_user_file(str(tp), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(SCRIPT_DIR/"credentials.json"), SCOPES)
            creds = flow.run_local_server(port=0)
        tp.write_text(creds.to_json(), encoding="utf-8")
    return build("calendar", "v3", credentials=creds)

def get_or_create_calendar(service, calendar_name: str) -> str:
    page_token=None
    while True:
        resp = service.calendarList().list(pageToken=page_token, maxResults=250).execute()
        for item in resp.get("items", []):
            if item.get("summary") == calendar_name:
                return item["id"]
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    created = service.calendars().insert(body={"summary": calendar_name, "timeZone": "Europe/Zurich"}).execute()
    return created["id"]

def rfc3339_utc(ts: pd.Timestamp) -> str:
    if ts.tzinfo is None:
        ts = ts.tz_localize(LOCAL_TZ)
    return ts.tz_convert("UTC").isoformat().replace("+00:00", "Z")

def fingerprint(row) -> str:
    base = f"{row['Von'].isoformat()}|{row['Bis'].isoformat()}|{row.get('Raum','')}|{row.get('Standort','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def delete_future_own_events(service, calendar_id: str, horizon_days: int = 8) -> None:
    now_utc = pd.Timestamp.now(tz="UTC").isoformat()
    max_utc = (pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=horizon_days)).isoformat()
    to_delete=[]; page_token=None
    while True:
        resp = service.events().list(calendarId=calendar_id, timeMin=now_utc, timeMax=max_utc, singleEvents=True, maxResults=2500, pageToken=page_token).execute()
        for ev in resp.get("items", []):
            props = ev.get("extendedProperties", {}).get("private", {})
            if props.get("source") == SOURCE_TAG:
                to_delete.append(ev["id"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    for eid in to_delete:
        service.events().delete(calendarId=calendar_id, eventId=eid, sendUpdates="none").execute()
    if to_delete:
        print(f"[GCAL] Alte Events geloescht: {len(to_delete)}")

def push_events(service, calendar_id: str, df: pd.DataFrame) -> None:
    if df.empty:
        print("[GCAL] Keine Events zu pushen.")
        return
    inserted=0
    for _, r in df.iterrows():
        parts=["Belegt"]
        rc=str(r.get("Raumcode") or "").strip()
        st=str(r.get("Standort") or "").strip()
        if rc: parts.append(rc)
        if st: parts.append(st)
        summary=" - ".join(parts)
        loc=str(r.get("Raum") or "")
        loc = f"{loc} | {st}" if st and loc else (st or loc)
        body={
            "summary": summary,
            "location": loc,
            "start": {"dateTime": rfc3339_utc(r["Von"]), "timeZone": "UTC"},
            "end":   {"dateTime": rfc3339_utc(r["Bis"]), "timeZone": "UTC"},
            "visibility":"private","transparency":"opaque",
            "extendedProperties": {"private": {"source": SOURCE_TAG, "fp": fingerprint(r)}}
        }
        service.events().insert(calendarId=calendar_id, body=body, sendUpdates="none").execute()
        inserted+=1
    print(f"[GCAL] Eingetragen: {inserted} Events")

def _bucket_from_standort(standort: str, mode: str) -> str:
    s=(standort or "").strip()
    if not s:
        return "Unbekannt"
    if mode=="gebaeude":
        return s.split(" - ")[0].strip() or "Unbekannt"
    return s

def _calendar_name_for_bucket(base_name: str, bucket: str) -> str:
    name = f"{base_name} - {bucket.strip()}"
    return name[:80]

def group_and_push_by_calendar(service, base_calendar_name: str, df: pd.DataFrame, split_by: str) -> None:
    if df.empty:
        print("[GCAL] Keine Events zu pushen.")
        return
    if split_by == "none":
        cal_id = get_or_create_calendar(service, base_calendar_name)
        delete_future_own_events(service, cal_id, horizon_days=8)
        push_events(service, cal_id, df)
        return
    mode = "standort" if split_by == "standort" else "gebaeude"
    work = df.copy()
    work["__bucket"] = work["Standort"].apply(lambda s: _bucket_from_standort(s, mode))
    for bucket, part in work.groupby("__bucket", dropna=False):
        cal_name = _calendar_name_for_bucket(base_calendar_name, str(bucket))
        cal_id = get_or_create_calendar(service, cal_name)
        delete_future_own_events(service, cal_id, horizon_days=8)
        push_events(service, cal_id, part.drop(columns=["__bucket"], errors="ignore"))

# ------------------ Login & Filter & Grid ------------------

def send_alert_email(subject: str, body: str, to_addr: Optional[str] = None) -> None:
    host = os.getenv("SMTP_HOST", "smtp.office365.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER", "")
    pwd  = os.getenv("SMTP_PASS", "")
    from_addr = os.getenv("SMTP_FROM", user or "no-reply@localhost")
    to_addr = to_addr or os.getenv("ALERT_TO", "gec5@bfh.ch")

    if not user or not pwd:
        print("[ALERT] SMTP_USER/SMTP_PASS fehlen – keine Mail gesendet.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port) as s:
            s.ehlo()
            s.starttls(context=ssl.create_default_context())
            s.login(user, pwd)
            s.send_message(msg)
        print(f"[ALERT] Mail gesendet an {to_addr}")
    except Exception as e:
        print(f"[ALERT] Mailversand fehlgeschlagen: {e}")

async def ensure_logged_in(page: Page, timeout_ms: int) -> bool:
    """Auto-Login (AzureAD & klassische Form) ueber ROOMS_USER/ROOMS_PASS. Nutzt persistentes Profil."""
    user = os.getenv("ROOMS_USER") or ""
    pw = os.getenv("ROOMS_PASS") or ""

    # schon eingeloggt?
    try:
        if await page.locator("a:has-text('Abmelden'), a:has-text('Logout'), a:has-text('Profil')").first.is_visible():
            return True
    except Exception:
        pass

    await page.wait_for_timeout(800)

    # Azure AD
    try:
        if await page.locator("#i0116").is_visible():  # E-Mail
            if user:
                await page.fill("#i0116", user)
                await page.click("#idSIButton9")
                await page.wait_for_timeout(600)
        if await page.locator("#i0118").is_visible():  # Passwort
            if pw:
                await page.fill("#i0118", pw)
                await page.click("#idSIButton9")
                await page.wait_for_timeout(1200)
        if await page.locator("#KmsiCheckbox, input[name='DontShowAgain']").first.is_visible():
            try:
                await page.check("#KmsiCheckbox")
            except Exception:
                pass
            for sel in ["#idSIButton9","button:has-text('Ja')","input[type='submit']:has-text('Ja')"]:
                try:
                    if await page.locator(sel).is_visible():
                        await page.click(sel)
                        await page.wait_for_timeout(600)
                        break
                except Exception:
                    pass
    except Exception:
        pass

    # Klassische Form
    try:
        user_sel = "input[name='username'], input[type='email'], input[name='UserName']"
        pass_sel = "input[type='password'], input[name='Password']"
        if await page.locator(user_sel).first.is_visible():
            if user:
                await page.fill(user_sel, user)
            if await page.locator(pass_sel).first.is_visible() and pw:
                await page.fill(pass_sel, pw)
            for sel in ["button[type='submit']","button:has-text('Anmelden')","input[type='submit']"]:
                try:
                    if await page.locator(sel).first.is_visible():
                        await page.click(sel)
                        break
                except Exception:
                    pass
            await page.wait_for_timeout(1000)
    except Exception:
        pass

    # zurueck zur Zielliste
    try:
        await page.goto(URL_FIND, wait_until="domcontentloaded")
    except Exception:
        pass

    # finaler Login-Check
    try:
        if await page.locator("a:has-text('Abmelden'), a:has-text('Logout'), a:has-text('Profil')").first.is_visible():
            return True
    except Exception:
        pass
    return False

async def wait_for_results_frame(page: Page, timeout_ms: int) -> Frame:
    sel_candidates = [
        "div.k-grid-content table tbody tr",
        "div.k-grid-content table tr",
        "#contentgrid table tbody tr",
        "table.k-selectable tbody tr",
        "table tbody tr",
    ]
    for sel in sel_candidates:
        try:
            await page.wait_for_selector(sel, timeout=min(4000, timeout_ms), state="visible")
            return page.main_frame
        except Exception:
            pass
    for fr in page.frames:
        for sel in sel_candidates:
            try:
                await fr.wait_for_selector(sel, timeout=2000, state="visible")
                return fr
            except Exception:
                continue
    return page.main_frame

async def extract_kendo_grid(frame: Frame) -> pd.DataFrame:
    headers = await frame.eval_on_selector_all("div.k-grid-header thead tr th", "els => els.map(th => th.innerText.trim())")
    rows = await frame.eval_on_selector_all("div.k-grid-content table tbody tr", "els => els.map(tr => Array.from(tr.children).map(td => td.innerText.trim()))")
    if not rows:
        rows = await frame.eval_on_selector_all("table tbody tr", "els => els.map(tr => Array.from(tr.children).map(td => td.innerText.trim()))")
    if not rows:
        return pd.DataFrame()
    if not headers:
        maxlen = max(len(r) for r in rows)
        headers = [str(i) for i in range(maxlen)]
    width = len(headers)
    norm_rows = [r + [""] * (width - len(r)) if len(r) < width else r[:width] for r in rows]
    return pd.DataFrame(norm_rows, columns=headers)

async def kendo_click_next(frame: Frame) -> bool:
    sels = ["a[aria-label*='next' i]", "a[title*='Weiter' i]", "a[title*='Naechste' i]", "a.k-pager-next", "a.k-link.k-pager-next", "button.k-pager-next"]
    for sel in sels:
        try:
            el = await frame.wait_for_selector(sel, timeout=800)
            if not el:
                continue
            disabled = await el.get_attribute("aria-disabled")
            cls = (await el.get_attribute("class")) or ""
            if disabled == "true" or "k-disabled" in cls or "k-state-disabled" in cls:
                return False
            await el.click()
            return True
        except Exception:
            continue
    return False

async def try_set_page_size(frame: Frame, target: int = 200) -> None:
    try:
        sel = await frame.query_selector("select.k-pager-sizes")
        if sel:
            try:
                await sel.select_option(str(target))
                return
            except Exception:
                pass
    except Exception:
        pass
    try:
        dd = await frame.query_selector(".k-pager-sizes .k-dropdown, .k-pager-sizes .k-combobox")
        if dd:
            await dd.click()
            opt = await frame.wait_for_selector(f".k-list .k-item:has-text('{target}')", timeout=1200)
            if opt:
                await opt.click()
    except Exception:
        pass

async def collect_all_pages(page: Page, timeout_ms: int, max_pages: int = 200) -> pd.DataFrame:
    frame = await wait_for_results_frame(page, timeout_ms)
    await try_set_page_size(frame, 200)
    await page.wait_for_timeout(600)
    all_dfs: List[pd.DataFrame] = []
    seen_first_row: set[str] = set()
    for _ in range(max_pages):
        dfp = await extract_kendo_grid(frame)
        if dfp is None or dfp.empty:
            break
        try:
            first_row = "|".join(map(str, dfp.iloc[0].tolist()))
        except Exception:
            first_row = f"len={len(dfp)}"
        if first_row in seen_first_row:
            break
        seen_first_row.add(first_row)
        all_dfs.append(dfp)
        clicked = await kendo_click_next(frame)
        if not clicked:
            break
        await page.wait_for_timeout(600)
    if not all_dfs:
        return pd.DataFrame()
    raw = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
    return raw

async def verify_window_matches(page: Page, timeout_ms: int) -> bool:
    try:
        frame = await wait_for_results_frame(page, timeout_ms)
        df = await extract_kendo_grid(frame)
        if df is None or df.empty:
            return False
        lower = {str(c).lower(): c for c in df.columns}
        col_von = next((lower[k] for k in lower if ("von" in k) or ("beginn" in k) or ("start" in k)), None)
        col_bis = next((lower[k] for k in lower if ("bis" in k) or ("ende" in k) or ("end" in k)), None)
        if not (col_von and col_bis):
            return False
        def pdt(x):
            s = str(x).strip().replace(","," ")
            return pd.to_datetime(s, dayfirst=True, errors="coerce")
        probe = df.head(60).copy()
        probe["__v"] = probe[col_von].apply(pdt)
        probe["__b"] = probe[col_bis].apply(pdt)
        probe = probe[pd.notna(probe["__v"]) & pd.notna(probe["__b"])]
        if probe.empty:
            return False
        start, end = compute_window_days_7()
        ratio = ((probe["__b"] >= start) & (probe["__v"] <= end)).mean()
        return bool(ratio >= 0.6)
    except Exception:
        return False

async def ensure_window_or_retry(page: Page, timeout_ms: int, attempts: int = 3) -> bool:
    ok=False
    for i in range(attempts):
        von_ok, bis_ok, searched, tag = await apply_7day_filter_and_search(page)
        print(f"Filter gesetzt (Try {i+1}/{attempts}): Von={von_ok} Bis={bis_ok} | Suche ausgeloest={searched} | {tag}")
        await page.wait_for_timeout(1200)
        if await verify_window_matches(page, timeout_ms):
            ok=True
            print("[FILTER] Zeitfenster verifiziert.")
            break
        try:
            btn_reset = page.locator("button:has-text('Zuruecksetzen'), button:has-text('Zurücksetzen'), button:has-text('Reset')").first
            if await btn_reset.is_visible():
                await btn_reset.click()
        except Exception:
            pass
    if not ok:
        print("[FILTER] Zeitfenster nicht sicher - fahre mit Grid-Scraping fort (lokale Filterung heute->+7).")
    return ok

async def apply_7day_filter_and_search(page: Page) -> Tuple[bool, bool, bool, str]:
    """Setzt Datum/Zeit (heute -> +7, 06:00-22:00), deaktiviert 'Heute...' und klickt Finden via JS."""
    start, end = compute_window_days_7()
    try:
        res = await page.evaluate(
            """(arg) => {
                const { sDate, eDate } = arg;
                const fmt = (iso) => {
                  const d = new Date(iso);
                  const dd = String(d.getDate()).padStart(2,'0');
                  const mm = String(d.getMonth()+1).padStart(2,'0');
                  const yyyy = d.getFullYear();
                  return `${dd}.${mm}.${yyyy}`;
                };
                const valVon = fmt(sDate), valBis = fmt(eDate);

                const manual = document.querySelector('#ReservationFilter_ManualDateTimeSelection');
                if (manual && !manual.checked){ manual.checked=true; manual.dispatchEvent(new Event('change',{bubbles:true})); }

                const alt = document.querySelector('#ReservationFilter_Reservationszeitpunkt');
                let untoggled=false; if (alt && alt.value!=='0'){ alt.value='0'; alt.dispatchEvent(new Event('change',{bubbles:true})); untoggled=true; }

                const setVal=(sel,val)=>{ const el=document.querySelector(sel); if(!el) return false; el.removeAttribute('readonly'); el.value=val; el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})); return true; };
                let vonOk=setVal('#dtpReservationDateFrom', valVon);
                let bisOk=setVal('#dtpReservationDateTo', valBis);
                setVal('#dtpReservationTimeFrom','06:00');
                setVal('#dtpReservationTimeTo','22:00');

                let searched=false;
                const btn = Array.from(document.querySelectorAll('button,input[type="button"],input[type="submit"]'))
                                  .find(b=>/finden|suchen/i.test((b.textContent||b.value||'')));
                if(btn){ btn.click(); searched=true; }
                return {vonOk,bisOk,searched,untoggled};
            }""",
            {"sDate": start.isoformat(), "eDate": end.isoformat()},
        )
        return (bool(res["vonOk"]), bool(res["bisOk"]), bool(res["searched"]), ("untoggled" if res.get("untoggled") else "no-alt"))
    except Exception as e:
        return False, False, False, f"err={e}"

async def force_find(page: Page, timeout_ms: int) -> None:
    # Popups schliessen (Kalender)
    for _ in range(3):
        try:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(120)
        except Exception:
            pass
    try:
        await page.click("body", position={"x": 10, "y": 10})
    except Exception:
        pass

    # Vorherige erste Tabellenzeile merken
    before = ""
    try:
        frame = await wait_for_results_frame(page, timeout_ms)
        df = await extract_kendo_grid(frame)
        if df is not None and not df.empty:
            before = "|".join(map(str, df.iloc[0].tolist()))
    except Exception:
        pass

    # "Finden" wirklich klicken
    selectors = [
        "button:has-text('Finden')",
        "input[type='submit'][value*='Finden' i]",
        "#btnFind",
        "button[name*='Find' i]"
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible():
                await loc.click()
                break
        except Exception:
            continue

    # Auf Grid-Update warten
    try:
        frame = await wait_for_results_frame(page, timeout_ms)
        async def grid_changed() -> bool:
            d2 = await extract_kendo_grid(frame)
            if d2 is None or d2.empty:
                return False
            now = "|".join(map(str, d2.iloc[0].tolist()))
            return now != before
        for _ in range(40):  # ~12s
            if await grid_changed():
                return
            await page.wait_for_timeout(300)
    except Exception:
        pass

# ------------------ Main ------------------

async def run(timeout_ms: int,
              calendar: str,
              downloads_override: Optional[str],
              split_by: str,
              chunk: bool,
              prefer_grid: bool) -> None:
    downloads_dir = Path(downloads_override) if downloads_override else (Path.home() / "Downloads")
    start, end = compute_window_days_7()
    print(f"Zeitraum: {start.strftime('%d.%m.%Y %H:%M')} -> {end.strftime('%d.%m.%Y %H:%M')}")

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            headless=False,
            accept_downloads=True,
        )
        page = await context.new_page()
        await page.goto(URL_FIND, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 10000))
        except PWTimeoutError:
            pass

        # Auto-Login
        logged_in = await ensure_logged_in(page, timeout_ms)
        if not logged_in:
            ts = pd.Timestamp.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
            send_alert_email(
                subject="BFH Rooms Sync: Login fehlgeschlagen",
                body=f"Automatischer Login ist fehlgeschlagen ({ts}). Bitte pruefen.",
                to_addr=os.getenv("ALERT_TO", "gec5@bfh.ch"),
            )
            print("[LOGIN] Fehlgeschlagen - Abbruch.")
            await context.close()
            return

        try:
            await page.wait_for_load_state("networkidle", timeout=4000)
        except Exception:
            pass

        # Filter setzen
        von_ok, bis_ok, searched, info = await apply_7day_filter_and_search(page)
        print(f"[FILTER] JS: vonOk={von_ok} bisOk={bis_ok} searched={searched} {info}")

        # Popups schliessen und Finden erzwingen
        await force_find(page, timeout_ms)

        _ = await ensure_window_or_retry(page, timeout_ms)

        raw = pd.DataFrame()

        # Export (optional)
        dest = None
        if not prefer_grid:
            dest = await click_export_and_download(page, timeout_ms, downloads_dir)
            if dest and dest.suffix.lower() == ".csv":
                try:
                    raw = read_csv_smart(dest)
                    print(f"[CSV] Spalten: {list(raw.columns)}")
                except Exception as e:
                    print(f"Konnte CSV nicht parsen: {e}")

        # Grid-Scrape (direkt oder Fallback)
        if raw.empty:
            _ = await wait_for_results_frame(page, timeout_ms)
            (ARTIFACTS_DIR / "after_find.html").write_text(await page.content(), encoding="utf-8")
            raw = await collect_all_pages(page, timeout_ms)
            try:
                print(f"[GRID] Spalten: {list(raw.columns)}")
                print("[GRID] Kopf (5 Zeilen):")
                print(raw.head(5).to_string(index=False))
            except Exception:
                pass

        await context.close()

    if raw is None or raw.empty:
        print("[SCRAPE] Keine Daten - Abbruch ohne Google-Kalender.")
        return

    print(f"[SCRAPE] Rohzeilen (alle Seiten): {len(raw)}")
    df = normalize_to_room_times(raw)
    if df.empty:
        print("[NORM] Keine (Von, Bis, Raum) Zeilen im 7-Tage-Fenster.")
        return

    if chunk:
        df = chunk_to_days_6_22(df)

    print(f"[NORM] Zeilen im Fenster: {len(df)}")
    try:
        print("[NORM] Vorschau:")
        print(df.head(10).to_string(index=False))
    except Exception:
        pass

    # HTML-Tafel
    export_html_timeline(df, ARTIFACTS_DIR / "schedule.html")

    # Google push
    svc = load_gcal_service()
    group_and_push_by_calendar(svc, calendar, df, split_by)
    print("Sync fertig.")

# ------------------ CLI ------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=180000, help="Timeout fuer Export/Grid in Millisekunden")
    parser.add_argument("--calendar", default="Rooms_BFH_Kilchenmann", help="Basiskalendername")
    parser.add_argument("--downloads", default=None, help="Optionaler Pfad zum Downloads-Ordner")
    parser.add_argument("--split-by", choices=["none", "standort", "gebaeude"], default="none", help="Kalender-Aufteilung")
    parser.add_argument("--no-chunk", action="store_true", help="Deaktiviert Tages-Splitting 06-22")
    parser.add_argument("--prefer-grid", action="store_true", help="Export ueberspringen und direkt Grid scrapen")
    parser.add_argument("--alert-to", default=None, help="E-Mail fuer Login-Fehler (override)")

    args = parser.parse_args()
    if args.alert_to:
        os.environ["ALERT_TO"] = args.alert_to

    asyncio.run(
        run(
            args.timeout,
            args.calendar,
            args.downloads,
            args.split_by,
            chunk=(not args.no_chunk),
            prefer_grid=args.prefer_grid,
        )
    )
