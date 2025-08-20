import os
import logging
import sys
import time

# Script-Verzeichnis zum Pfad hinzufügen
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

# Logging konfigurieren
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from notion_service import get_all_aufgaben, get_all_offerten, get_all_rechnungen, get_all_meetings
    logger.info("Notion-Service-Module erfolgreich importiert")
except ImportError as e:
    logger.error(f"Fehler beim Importieren von notion_service: {e}")
    logger.error(f"Verfügbare Dateien im Verzeichnis {script_dir}: {os.listdir(script_dir)}")
    raise

try:
    from supabaseapi import speichere_aufgaben, speichere_offerten, speichere_rechnungen, speichere_meetings
    logger.info("Supabase-API-Module erfolgreich importiert")
except ImportError as e:
    logger.error(f"Fehler beim Importieren von supabaseapi: {e}")
    logger.error(f"Verfügbare Dateien im Verzeichnis {script_dir}: {os.listdir(script_dir)}")
    raise

def sync_aufgaben_from_notion_to_supabase():
    """Synchronisiert Aufgaben von Notion nach Supabase."""
    start_time = time.time()
    logger.info("================================")
    logger.info("Starte Synchronisierung: Aufgaben")
    logger.info("================================")
    
    try:
        aufgaben = get_all_aufgaben()
        logger.info(f"Abgerufene Aufgaben: {len(aufgaben)}")
        
        if aufgaben:
            speichere_aufgaben(aufgaben)
            logger.info(f"{len(aufgaben)} Aufgaben synchronisiert.")
        else:
            logger.warning("Keine Aufgaben zum Synchronisieren gefunden.")
            
        end_time = time.time()
        logger.info(f"Aufgaben-Synchronisierung abgeschlossen in {end_time - start_time:.2f} Sekunden")
        return aufgaben
    except Exception as e:
        logger.error(f"Fehler bei der Aufgaben-Synchronisation: {e}", exc_info=True)
        return []

def sync_offerten_from_notion_to_supabase():
    """Synchronisiert Offerten von Notion nach Supabase."""
    start_time = time.time()
    logger.info("================================")
    logger.info("Starte Synchronisierung: Offerten")
    logger.info("================================")
    
    try:
        offerten = get_all_offerten()
        logger.info(f"Abgerufene Offerten: {len(offerten)}")
        
        if offerten:
            # Zeige Details zum ersten Eintrag
            if len(offerten) > 0:
                logger.debug(f"Erste Offerte: {offerten[0]}")
                
            speichere_offerten(offerten)
            logger.info(f"{len(offerten)} Offerten synchronisiert.")
        else:
            logger.warning("Keine Offerten zum Synchronisieren gefunden.")
            
        end_time = time.time()
        logger.info(f"Offerten-Synchronisierung abgeschlossen in {end_time - start_time:.2f} Sekunden")
        return offerten
    except Exception as e:
        logger.error(f"Fehler bei der Offerten-Synchronisation: {e}", exc_info=True)
        return []

def sync_rechnungen_from_notion_to_supabase():
    """Synchronisiert Rechnungen von Notion nach Supabase."""
    start_time = time.time()
    logger.info("==================================")
    logger.info("Starte Synchronisierung: Rechnungen")
    logger.info("==================================")
    
    try:
        rechnungen = get_all_rechnungen()
        logger.info(f"Abgerufene Rechnungen: {len(rechnungen)}")
        
        if rechnungen:
            # Zeige Details zum ersten Eintrag
            if len(rechnungen) > 0:
                logger.debug(f"Erste Rechnung: {rechnungen[0]}")
                
            speichere_rechnungen(rechnungen)
            logger.info(f"{len(rechnungen)} Rechnungen synchronisiert.")
        else:
            logger.warning("Keine Rechnungen zum Synchronisieren gefunden.")
            
        end_time = time.time()
        logger.info(f"Rechnungen-Synchronisierung abgeschlossen in {end_time - start_time:.2f} Sekunden")
        return rechnungen
    except Exception as e:
        logger.error(f"Fehler bei der Rechnungen-Synchronisation: {e}", exc_info=True)
        return []

def sync_meetings_from_notion_to_supabase():
    """Synchronisiert Meetings von Notion nach Supabase."""
    start_time = time.time()
    logger.info("================================")
    logger.info("Starte Synchronisierung: Meetings")
    logger.info("================================")
    
    try:
        meetings = get_all_meetings()
        logger.info(f"Abgerufene Meetings: {len(meetings)}")
        
        if meetings:
            # Zeige Details zum ersten Eintrag
            if len(meetings) > 0:
                logger.debug(f"Erstes Meeting: {meetings[0]}")
                
            speichere_meetings(meetings)
            logger.info(f"{len(meetings)} Meetings synchronisiert.")
        else:
            logger.warning("Keine Meetings zum Synchronisieren gefunden.")
            
        end_time = time.time()
        logger.info(f"Meetings-Synchronisierung abgeschlossen in {end_time - start_time:.2f} Sekunden")
        return meetings
    except Exception as e:
        logger.error(f"Fehler bei der Meetings-Synchronisation: {e}", exc_info=True)
        return []

def synchronisiere_notion_daten():
    """Synchronisiert alle Notion-Daten mit Supabase."""
    start_time = time.time()
    logger.info("==========================================")
    logger.info("Starte vollständige Notion-Synchronisation")
    logger.info("==========================================")
    
    aufgaben = []
    offerten = []
    rechnungen = []
    meetings = []
    
    try:
        aufgaben = sync_aufgaben_from_notion_to_supabase()
    except Exception as e:
        logger.error(f"Fehler bei der Aufgaben-Synchronisation: {e}")
    
    try:
        offerten = sync_offerten_from_notion_to_supabase()
    except Exception as e:
        logger.error(f"Fehler bei der Offerten-Synchronisation: {e}")
    
    try:
        rechnungen = sync_rechnungen_from_notion_to_supabase()
    except Exception as e:
        logger.error(f"Fehler bei der Rechnungen-Synchronisation: {e}")
    
    try:
        meetings = sync_meetings_from_notion_to_supabase()
    except Exception as e:
        logger.error(f"Fehler bei der Meetings-Synchronisation: {e}")
    
    end_time = time.time()
    logger.info("============= Synchronisation abgeschlossen =============")
    logger.info(f"Gesamt: {len(aufgaben)} Aufgaben, {len(offerten)} Offerten, " +
                f"{len(rechnungen)} Rechnungen, {len(meetings)} Meetings")
    logger.info(f"Gesamtzeit: {end_time - start_time:.2f} Sekunden")
    logger.info("========================================================")

def test_specific_sync():
    """Testet nur die Synchronisierung von Offerten und Meetings."""
    logger.info("Starte Test-Synchronisierung für Offerten und Meetings...")
    
    try:
        offerten = sync_offerten_from_notion_to_supabase()
        logger.info(f"Offerten-Synchronisierung: {len(offerten)} Einträge")
    except Exception as e:
        logger.error(f"Fehler bei der Offerten-Test-Synchronisation: {e}")
    
    try:
        meetings = sync_meetings_from_notion_to_supabase()
        logger.info(f"Meetings-Synchronisierung: {len(meetings)} Einträge")
    except Exception as e:
        logger.error(f"Fehler bei der Meetings-Test-Synchronisation: {e}")
    
    logger.info("Test-Synchronisierung abgeschlossen")

if __name__ == "__main__":
    # Logging-Format für die Konsole
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Führe nur die Synchronisierung für Offerten und Meetings aus (zum Testen)
    test_specific_sync()
    
    # Oder die vollständige Synchronisierung
    # synchronisiere_notion_daten()
