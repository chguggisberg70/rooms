#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Probe-Script für BFH 3V-ROOMS Listenfilter:
- öffnet die Listen-URL
- setzt von/bis + Zeiten zuverlässig (inkl. Datepicker-Fallback für "bis")
- klickt "Finden"
- macht Screenshot
"""

import argparse
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

# ---- Einstellungen anpassen ----
BASE_URL = "https://bfh.book.3vrooms.app/Default/Lists/Reservation/FindReservation?filterSettingId=31035"


def log(msg: str):
    print(msg, flush=True)


def set_input_and_confirm(locator, text: str):
    """Schreibt Text in ein Input-Feld und bestätigt mit Enter+Blur."""
    locator.click()
    try:
        locator.press("Control+a")
    except:
        locator.press("Meta+a")
    locator.type(text, delay=20)
    locator.press("Enter")
    locator.blur()


def run(cfg):
    start = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=cfg.days - 1)

    log(f"Zeitraum: {start:%d.%m.%Y} 00:00 -> {end:%d.%m.%Y} 23:59")
    log(f"[PROBE] Ziel-Zeitraum: {start:%d.%m.%Y} → {end:%d.%m.%Y} ({cfg.t1}–{cfg.t2})")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not cfg.headed)
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()

        # 1) Seite öffnen
        page.goto(BASE_URL, timeout=cfg.timeout)
        page.wait_for_load_state("domcontentloaded", timeout=cfg.timeout)
        page.wait_for_load_state("networkidle", timeout=cfg.timeout)

        # 2) Falls vorhanden: Zurücksetzen klicken
        reset_btn = page.get_by_role("button", name="Zurücksetzen").first
        try:
            if reset_btn.is_visible(timeout=1500):
                reset_btn.click()
                page.wait_for_load_state("domcontentloaded", timeout=cfg.timeout)
                page.wait_for_load_state("networkidle", timeout=cfg.timeout)
                log("[INFO] Filter zurückgesetzt.")
        except Exception:
            log("[INFO] Kein Zurücksetzen-Button gefunden.")

        # 3) Panel "Datum/Zeit" finden
        panel = page.locator("text=Datum/Zeit").locator("xpath=..").first
        panel.wait_for(timeout=cfg.timeout)
        date_inputs = panel.locator("input[type='text']")
        date_inputs.first.wait_for(timeout=cfg.timeout)

        von_input = date_inputs.nth(0)
        bis_input = date_inputs.nth(1)
        t1_input  = date_inputs.nth(2)
        t2_input  = date_inputs.nth(3)

        fmt = "%d.%m.%Y"

        # 4) Von-Datum setzen
        set_input_and_confirm(von_input, start.strftime(fmt))

        # 5) Bis-Datum setzen – zuerst per Input, sonst Fallback über Datepicker
        try:
            set_input_and_confirm(bis_input, end.strftime(fmt))
            log(f"[PROBE] Bis-Datum via Input gesetzt: {end:%d.%m.%Y}")
        except Exception as e:
            log(f"[WARN] Direktes Setzen vom bis-Datum fehlgeschlagen ({e}) – versuche Datepicker…")
            cal_icons = panel.locator("xpath=.//img[contains(@src,'calendar')]")
            if cal_icons.count() >= 2:
                cal_icons.nth(1).click()
                # Warte bis Kalender sichtbar
                page.locator("//table[contains(@class,'calendar')]").first.wait_for(timeout=cfg.timeout)
                # Tag im sichtbaren Kalender anklicken
                page.locator(f"//td[normalize-space()='{end.day}']").first.click()
                log(f"[PROBE] Bis-Datum via Datepicker gewählt: {end:%d.%m.%Y}")
            else:
                log("[FEHLER] Konnte Datepicker-Icon nicht finden – bitte Selektor anpassen.")

        # 6) Zeiten setzen
        set_input_and_confirm(t1_input, cfg.t1)
        set_input_and_confirm(t2_input, cfg.t2)

        log(f"[PROBE] UI: von={von_input.input_value()} bis={bis_input.input_value()} "
            f"t1={t1_input.input_value()} t2={t2_input.input_value()}")

        # 7) Suchen
        page.get_by_role("button", name="Finden").click()
        page.wait_for_load_state("networkidle", timeout=cfg.timeout)

        # 8) Warten bis Tabelle da ist
        page.locator("table").first.wait_for(timeout=cfg.timeout)

        # 9) Screenshot
        page.screenshot(path=cfg.screenshot, full_page=True)
        log(f"[PROBE] Screenshot: {cfg.screenshot}")

        browser.close()
        log("✅ Probe beendet.")


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=5, help="Anzahl Tage inkl. Start")
    ap.add_argument("--t1", default="06:00")
    ap.add_argument("--t2", default="22:00")
    ap.add_argument("--timeout", type=int, default=240000)
    ap.add_argument("--screenshot", default="artifacts_sync/probe_after_find.png")
    ap.add_argument("--headed", action="store_true", help="Browser sichtbar öffnen")
    return ap.parse_args()


if __name__ == "__main__":
    cfg = parse_args()
    run(cfg)