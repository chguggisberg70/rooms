import time
import os
import sys
import logging

# Script-Verzeichnis zum Pfad hinzufügen
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Logs auch in die Konsole ausgeben
    ]
)
logger = logging.getLogger(__name__)

# Lade Umgebungsvariablen
try:
    from dotenv import load_dotenv
    env_path = os.path.join(script_dir, '.well-known', '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        logger.info(f"Umgebungsvariablen aus {env_path} geladen")
    else:
        logger.warning(f"Umgebungsvariablen-Datei {env_path} nicht gefunden!")
        
        # Versuche alternative Pfade
        alt_paths = [
            os.path.join(os.getcwd(), '.well-known', '.env'),
            os.path.join(os.path.dirname(os.getcwd()), '.well-known', '.env'),
            '.env'
        ]
        for path in alt_paths:
            if os.path.exists(path):
                load_dotenv(path)
                logger.info(f"Umgebungsvariablen aus alternativer Quelle geladen: {path}")
                break
except Exception as e:
    logger.error(f"Fehler beim Laden der Umgebungsvariablen: {e}")

# Importiere die Synchronisationsfunktionen
try:
    from sync_functions import synchronisiere_notion_daten, test_specific_sync
    logger.info("Sync-Funktionen erfolgreich importiert")
except ImportError as e:
    logger.error(f"Fehler beim Importieren der Sync-Funktionen: {e}")
    sys.exit(1)

try:
    import notion_service
    logger.info("Notion-Service erfolgreich importiert")
except ImportError as e:
    logger.error(f"Fehler beim Importieren des Notion-Service: {e}")
    sys.exit(1)

try:
    import supabaseapi
    logger.info("Supabase-API erfolgreich importiert")
except ImportError as e:
    logger.error(f"Fehler beim Importieren der Supabase-API: {e}")
    sys.exit(1)

def run_sync():
    """Führt die Synchronisation aus."""
    start_time = time.time()
    logger.info("=================================================")
    logger.info("       STARTE NOTION-SUPABASE SYNCHRONISATION    ")
    logger.info("=================================================")
    
    # Zeige Umgebungsinformationen
    logger.info(f"Python Version: {sys.version}")
    logger.info(f"Arbeitsverzeichnis: {os.getcwd()}")
    logger.info(f"Skriptverzeichnis: {script_dir}")
    logger.info(f"Dateien im Skriptverzeichnis: {os.listdir(script_dir)}")
    
    # Zeige Notion-Datenbank-IDs
    logger.info(f"NOTION_DB_AUFGABEN: {os.getenv('NOTION_DB_AUFGABEN')}")
    logger.info(f"NOTION_DB_OFFERTEN: {os.getenv('NOTION_DB_OFFERTEN')}")
    logger.info(f"NOTION_DB_RECHNUNGEN: {os.getenv('NOTION_DB_RECHNUNGEN')}")
    logger.info(f"NOTION_DB_MEETINGS: {os.getenv('NOTION_DB_MEETINGS')}")
    
    # Prüfe die Verbindungen
    notion_ok = notion_service.test_connection()
    supabase_ok = supabaseapi.test_connection()
    
    if notion_ok and supabase_ok:
        logger.info("Beide Verbindungen erfolgreich.")
        
        # Test Supabase-Tabellenstrukturen
        supabaseapi.check_table_structure('aufgaben')
        supabaseapi.check_table_structure('notion_offerten')
        supabaseapi.check_table_structure('notion_rechnungen')
        supabaseapi.check_table_structure('notion_meetings')
        
        # Führe die Synchronisation aus
        try:
            # Zuerst nur Offerten und Meetings testen (häufigere Probleme)
            logger.info("Teste spezifische Synchronisierung für Offerten und Meetings...")
            test_specific_sync()
            
            # Dann vollständige Synchronisierung
            logger.info("Starte vollständige Synchronisierung...")
            synchronisiere_notion_daten()
            
            end_time = time.time()
            logger.info(f"Synchronisation erfolgreich abgeschlossen in {end_time - start_time:.2f} Sekunden.")
        except Exception as e:
            logger.error(f"Fehler bei der Synchronisation: {e}", exc_info=True)
    else:
        if not notion_ok:
            logger.error("Konnte keine Verbindung zu Notion herstellen.")
        if not supabase_ok:
            logger.error("Konnte keine Verbindung zu Supabase herstellen.")
        logger.error("Synchronisation abgebrochen.")
    
    logger.info("=================================================")
    logger.info("       ENDE DER SYNCHRONISATION                  ")
    logger.info("=================================================")

if __name__ == "__main__":
    # Führe die Synchronisation einmalig aus (für GitHub Actions)
    try:
        run_sync()
    except Exception as e:
        logger.error(f"Unbehandelte Ausnahme bei der Ausführung: {e}", exc_info=True)
        sys.exit(1)
