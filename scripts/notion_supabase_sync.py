#!/usr/bin/env python3
# notion_supabase_sync.py - Komplettes Script zur Synchronisierung von Notion mit Supabase

import os
import sys
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

# Logging-Konfiguration
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("notion-sync")

# Lade Umgebungsvariablen
try:
    from dotenv import load_dotenv
    env_paths = [
        '.well-known/.env',
        os.path.join(os.getcwd(), '.well-known', '.env'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.well-known', '.env')
    ]
    
    env_loaded = False
    for path in env_paths:
        if os.path.exists(path):
            load_dotenv(path)
            logger.info(f"Umgebungsvariablen aus {path} geladen")
            env_loaded = True
            break
    
    if not env_loaded:
        logger.warning("Keine .env-Datei gefunden. Verwende Umgebungsvariablen.")
except ImportError:
    logger.warning("python-dotenv nicht installiert. Verwende Umgebungsvariablen.")

# Prüfen der erforderlichen Umgebungsvariablen
required_vars = ["NOTION_TOKEN", "SUPABASE_URL", "SUPABASE_KEY"]
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    logger.error(f"Fehlende Umgebungsvariablen: {', '.join(missing_vars)}")
    logger.error("Die Synchronisierung kann nicht fortgesetzt werden.")
    sys.exit(1)

# Verfügbare Datenbankverknüpfungen
db_mappings = {
    "aufgaben": os.getenv("NOTION_DB_AUFGABEN"),
    "raeume": os.getenv("NOTION_DB_RAEUME"),
    "bruecke": os.getenv("NOTION_DB_BRUECKE"),
    "rechnungen": os.getenv("NOTION_DB_RECHNUNGEN"),
    "offerten": os.getenv("NOTION_DB_OFFERTEN"),
    "meetings": os.getenv("NOTION_DB_MEETINGS")
}

# Importiere die erforderlichen Pakete
try:
    from notion_client import Client as NotionClient
    import supabase
except ImportError as e:
    logger.error(f"Erforderliche Pakete fehlen: {e}")
    logger.error("Bitte führe 'pip install notion_client supabase' aus.")
    sys.exit(1)

# Globale Variablen für Clients
notion = None
supabase_client = None

###################
# Notion-Services #
###################

def init_notion():
    """Initialisiert den Notion-Client."""
    global notion
    try:
        notion_token = os.getenv("NOTION_TOKEN")
        if not notion_token:
            logger.error("NOTION_TOKEN nicht gefunden in Umgebungsvariablen.")
            return False
        
        notion = NotionClient(auth=notion_token)
        # Test der Verbindung
        notion.users.me()
        logger.info("Verbindung zu Notion hergestellt.")
        return True
    except Exception as e:
        logger.error(f"Fehler bei der Initialisierung des Notion-Clients: {e}")
        return False

def get_notion_database(database_id: str) -> List[Dict]:
    """
    Liest alle Einträge aus einer Notion-Datenbank.
    
    Args:
        database_id: ID der Notion-Datenbank
        
    Returns:
        Liste der Datenbankeinträge
    """
    global notion
    
    if not notion:
        if not init_notion():
            logger.error("Notion-Client konnte nicht initialisiert werden.")
            return []
    
    try:
        all_pages = []
        has_more = True
        next_cursor = None
        
        while has_more:
            response = notion.databases.query(
                database_id=database_id,
                start_cursor=next_cursor
            )
            
            all_pages.extend(response.get("results", []))
            
            has_more = response.get("has_more", False)
            next_cursor = response.get("next_cursor")
        
        logger.info(f"Erfolgreich {len(all_pages)} Einträge aus Notion-Datenbank abgerufen")
        return all_pages
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Notion-Datenbank: {e}")
        return []

def extract_aufgaben_from_notion(pages: List[Dict]) -> List[Dict]:
    """
    Extrahiert relevante Daten aus Notion-Aufgaben.
    
    Args:
        pages: Liste der Notion-Datenbankeinträge
        
    Returns:
        Liste von extrahierten Aufgaben-Daten
    """
    aufgaben = []
    
    for page in pages:
        try:
            aufgabe = {
                "id": page["id"].replace("-", ""),
                "name": extract_rich_text(page, "Name") or extract_title(page),
                "status": extract_select(page, "Status") or "",
                "fälligkeitsdatum": extract_date(page, "Fälligkeitsdatum") or "",
                "standort": extract_select(page, "Standort") or "",
                "priorität": extract_select(page, "Priorität") or "",
                "beschreibung": extract_rich_text(page, "Beschreibung") or "",
                "verantwortlich": extract_people(page, "Verantwortlich") or "",
                "updated_at": page.get("last_edited_time", "")
            }
            
            aufgaben.append(aufgabe)
            
            if len(aufgaben) == 1:
                logger.debug(f"Extrahierte Daten für ersten Eintrag: {aufgabe}")
                
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren einer Aufgabe: {e}")
    
    logger.info(f"Erfolgreich {len(aufgaben)} Aufgaben extrahiert")
    return aufgaben

def extract_offerten_from_notion(pages: List[Dict]) -> List[Dict]:
    """
    Extrahiert relevante Daten aus Notion-Offerten.
    
    Args:
        pages: Liste der Notion-Datenbankeinträge
        
    Returns:
        Liste von extrahierten Offerten-Daten
    """
    offerten = []
    
    for page in pages:
        try:
            offerte = {
                "id": page["id"].replace("-", ""),
                "name": extract_rich_text(page, "Name") or extract_title(page),
                "status": extract_select(page, "Status") or "",
                "datum": extract_date(page, "Datum") or "",
                "kunde_id": extract_relation(page, "Kunde") or "",
                "betrag": extract_number(page, "Betrag") or 0,
                "standort": extract_select(page, "Standort") or "",
                "beschreibung": extract_rich_text(page, "Beschreibung") or "",
                "verantwortlich": extract_people(page, "Verantwortlich") or "",
                "updated_at": page.get("last_edited_time", "")
            }
            
            offerten.append(offerte)
            
            if len(offerten) == 1:
                logger.debug(f"Extrahierte Daten für erste Offerte: {offerte}")
                
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren einer Offerte: {e}")
    
    logger.info(f"Erfolgreich {len(offerten)} Offerten extrahiert")
    return offerten

def extract_meetings_from_notion(pages: List[Dict]) -> List[Dict]:
    """
    Extrahiert relevante Daten aus Notion-Meetings.
    
    Args:
        pages: Liste der Notion-Datenbankeinträge
        
    Returns:
        Liste von extrahierten Meeting-Daten
    """
    meetings = []
    
    for page in pages:
        try:
            meeting = {
                "id": page["id"].replace("-", ""),
                "name": extract_rich_text(page, "Name") or extract_title(page),
                "status": extract_select(page, "Status") or "",
                "datum": extract_date(page, "Datum") or "",
                "uhrzeit": extract_rich_text(page, "Uhrzeit") or "",
                "teilnehmer": extract_people(page, "Teilnehmer") or "",
                "thema": extract_rich_text(page, "Thema") or "",
                "standort": extract_select(page, "Standort") or "",
                "notizen": extract_rich_text(page, "Notizen") or "",
                "updated_at": page.get("last_edited_time", "")
            }
            
            meetings.append(meeting)
            
            if len(meetings) == 1:
                logger.debug(f"Extrahierte Daten für erstes Meeting: {meeting}")
                
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren eines Meetings: {e}")
    
    logger.info(f"Erfolgreich {len(meetings)} Meetings extrahiert")
    return meetings

def extract_rechnungen_from_notion(pages: List[Dict]) -> List[Dict]:
    """
    Extrahiert relevante Daten aus Notion-Rechnungen.
    
    Args:
        pages: Liste der Notion-Datenbankeinträge
        
    Returns:
        Liste von extrahierten Rechnungs-Daten
    """
    rechnungen = []
    
    for page in pages:
        try:
            rechnung = {
                "id": page["id"].replace("-", ""),
                "name": extract_rich_text(page, "Name") or extract_title(page),
                "status": extract_select(page, "Status") or "",
                "datum": extract_date(page, "Datum") or "",
                "fälligkeitsdatum": extract_date(page, "Fälligkeitsdatum") or "",
                "kunde_id": extract_relation(page, "Kunde") or "",
                "betrag": extract_number(page, "Betrag") or 0,
                "bezahlt_am": extract_date(page, "Bezahlt am") or "",
                "standort": extract_select(page, "Standort") or "",
                "updated_at": page.get("last_edited_time", "")
            }
            
            rechnungen.append(rechnung)
            
            if len(rechnungen) == 1:
                logger.debug(f"Extrahierte Daten für erste Rechnung: {rechnung}")
                
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren einer Rechnung: {e}")
    
    logger.info(f"Erfolgreich {len(rechnungen)} Rechnungen extrahiert")
    return rechnungen

# Hilfsfunktionen zum Extrahieren von Notion-Eigenschaften
def extract_title(page: Dict) -> str:
    """Extrahiert den Titel einer Notion-Seite."""
    try:
        title_prop = next((prop for prop_name, prop in page.get("properties", {}).items() 
                          if prop.get("type") == "title"), None)
        
        if title_prop and title_prop.get("title"):
            title_parts = [part.get("plain_text", "") for part in title_prop.get("title", [])]
            return "".join(title_parts)
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren des Titels: {e}")
    
    return ""

def extract_rich_text(page: Dict, property_name: str) -> str:
    """Extrahiert Rich-Text aus einer Notion-Eigenschaft."""
    try:
        properties = page.get("properties", {})
        if property_name in properties and properties[property_name].get("type") == "rich_text":
            rich_text = properties[property_name].get("rich_text", [])
            return "".join([text.get("plain_text", "") for text in rich_text])
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren von Rich-Text '{property_name}': {e}")
    
    return ""

def extract_select(page: Dict, property_name: str) -> str:
    """Extrahiert eine Select-Eigenschaft aus einer Notion-Seite."""
    try:
        properties = page.get("properties", {})
        if property_name in properties and properties[property_name].get("type") == "select":
            select = properties[property_name].get("select", {})
            return select.get("name", "") if select else ""
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren von Select '{property_name}': {e}")
    
    return ""

def extract_date(page: Dict, property_name: str) -> str:
    """Extrahiert ein Datum aus einer Notion-Eigenschaft."""
    try:
        properties = page.get("properties", {})
        if property_name in properties and properties[property_name].get("type") == "date":
            date = properties[property_name].get("date", {})
            return date.get("start", "") if date else ""
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren von Datum '{property_name}': {e}")
    
    return ""

def extract_number(page: Dict, property_name: str) -> float:
    """Extrahiert eine Zahl aus einer Notion-Eigenschaft."""
    try:
        properties = page.get("properties", {})
        if property_name in properties and properties[property_name].get("type") == "number":
            return properties[property_name].get("number", 0) or 0
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren von Zahl '{property_name}': {e}")
    
    return 0

def extract_people(page: Dict, property_name: str) -> str:
    """Extrahiert Personen aus einer Notion-Eigenschaft."""
    try:
        properties = page.get("properties", {})
        if property_name in properties and properties[property_name].get("type") == "people":
            people = properties[property_name].get("people", [])
            return ", ".join([person.get("name", "") for person in people if "name" in person])
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren von Personen '{property_name}': {e}")
    
    return ""

def extract_relation(page: Dict, property_name: str) -> str:
    """Extrahiert eine Relation aus einer Notion-Eigenschaft."""
    try:
        properties = page.get("properties", {})
        if property_name in properties and properties[property_name].get("type") == "relation":
            relations = properties[property_name].get("relation", [])
            return relations[0].get("id", "").replace("-", "") if relations else ""
    except Exception as e:
        logger.error(f"Fehler beim Extrahieren von Relation '{property_name}': {e}")
    
    return ""

def get_all_aufgaben() -> List[Dict]:
    """Holt alle Aufgaben aus Notion."""
    database_id = os.getenv("NOTION_DB_AUFGABEN")
    if not database_id:
        logger.error("NOTION_DB_AUFGABEN nicht gesetzt in Umgebungsvariablen.")
        return []
    
    pages = get_notion_database(database_id)
    return extract_aufgaben_from_notion(pages)

def get_all_offerten() -> List[Dict]:
    """Holt alle Offerten aus Notion."""
    database_id = os.getenv("NOTION_DB_OFFERTEN")
    if not database_id:
        logger.error("NOTION_DB_OFFERTEN nicht gesetzt in Umgebungsvariablen.")
        return []
    
    pages = get_notion_database(database_id)
    return extract_offerten_from_notion(pages)

def get_all_meetings() -> List[Dict]:
    """Holt alle Meetings aus Notion."""
    database_id = os.getenv("NOTION_DB_MEETINGS")
    if not database_id:
        logger.error("NOTION_DB_MEETINGS nicht gesetzt in Umgebungsvariablen.")
        return []
    
    pages = get_notion_database(database_id)
    return extract_meetings_from_notion(pages)

def get_all_rechnungen() -> List[Dict]:
    """Holt alle Rechnungen aus Notion."""
    database_id = os.getenv("NOTION_DB_RECHNUNGEN")
    if not database_id:
        logger.error("NOTION_DB_RECHNUNGEN nicht gesetzt in Umgebungsvariablen.")
        return []
    
    pages = get_notion_database(database_id)
    return extract_rechnungen_from_notion(pages)

#####################
# Supabase-Services #
#####################

def init_supabase() -> bool:
    """Initialisiert den Supabase-Client."""
    global supabase_client
    
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            logger.error("SUPABASE_URL oder SUPABASE_KEY nicht gefunden in Umgebungsvariablen.")
            return False
        
        logger.debug(f"Verbinde zu Supabase mit URL: {supabase_url}")
        supabase_client = supabase.create_client(supabase_url, supabase_key)
        
        # Test der Verbindung durch eine einfache Abfrage
        try:
            logger.debug("Führe Testabfrage an Supabase durch...")
            response = supabase_client.table("notion_aufgaben").select("id").limit(1).execute()
            logger.debug(f"Supabase-Testabfrage erfolgreich: {response}")
        except Exception as e:
            logger.error(f"Fehler bei der Testabfrage: {e}")
            return False
        
        logger.info("Verbindung zu Supabase hergestellt.")
        return True
    except Exception as e:
        logger.error(f"Fehler bei der Initialisierung des Supabase-Clients: {e}")
        supabase_client = None
        return False

def get_supabase_client():
    """Gibt den Supabase-Client zurück oder initialisiert ihn bei Bedarf."""
    global supabase_client
    
    if not supabase_client:
        if not init_supabase():
            logger.error("Keine Verbindung zu Supabase.")
            return None
    
    return supabase_client

def sync_data_to_supabase(table_name: str, data: List[Dict[str, Any]]) -> bool:
    """
    Synchronisiert Daten zu einer Supabase-Tabelle.
    
    Args:
        table_name: Name der Tabelle in Supabase
        data: Liste der zu speichernden Datensätze
        
    Returns:
        True bei Erfolg, False bei Fehler
    """
    try:
        supabase = get_supabase_client()
        if not supabase:
            return False
        
        if not data:
            logger.info(f"Keine Daten zum Synchronisieren in Tabelle {table_name}.")
            return True
        
        logger.debug(f"Starte Synchronisierung von {len(data)} Einträgen zu {table_name}")
        
        # Für jedes Element einen Upsert durchführen
        success_count = 0
        for item in data:
            try:
                response = supabase.table(table_name).upsert(item).execute()
                success_count += 1
                if success_count % 10 == 0 or success_count == len(data):
                    logger.debug(f"Fortschritt: {success_count}/{len(data)} Elemente in {table_name} verarbeitet")
            except Exception as e:
                logger.error(f"Fehler beim Upsert in {table_name} für ID {item.get('id')}: {e}")
                # Fortfahren mit nächstem Element
        
        logger.info(f"Erfolgreich {success_count} von {len(data)} Einträgen in {table_name} synchronisiert.")
        return success_count > 0
    except Exception as e:
        logger.error(f"Fehler beim Synchronisieren von Daten zu {table_name}: {e}")
        return False

def speichere_aufgaben(aufgaben: List[Dict[str, Any]]) -> bool:
    """Speichert Aufgaben in Supabase."""
    logger.info(f"Speichere {len(aufgaben)} Aufgaben in Supabase...")
    return sync_data_to_supabase("notion_aufgaben", aufgaben)

def speichere_offerten(offerten: List[Dict[str, Any]]) -> bool:
    """Speichert Offerten in Supabase."""
    logger.info(f"Speichere {len(offerten)} Offerten in Supabase...")
    return sync_data_to_supabase("notion_offerten", offerten)

def speichere_meetings(meetings: List[Dict[str, Any]]) -> bool:
    """Speichert Meetings in Supabase."""
    logger.info(f"Speichere {len(meetings)} Meetings in Supabase...")
    return sync_data_to_supabase("notion_meetings", meetings)

def speichere_rechnungen(rechnungen: List[Dict[str, Any]]) -> bool:
    """Speichert Rechnungen in Supabase."""
    logger.info(f"Speichere {len(rechnungen)} Rechnungen in Supabase...")
    return sync_data_to_supabase("notion_rechnungen", rechnungen)

#################
# Hauptfunktion #
#################

def run_sync():
    """Führt die komplette Synchronisierung durch."""
    logger.info("Starte Notion-Supabase Synchronisierung...")
    
    # Initialisiere Verbindungen
    if not init_notion():
        logger.error("Notion-Verbindung konnte nicht hergestellt werden. Abbruch.")
        return False
    
    if not init_supabase():
        logger.error("Supabase-Verbindung konnte nicht hergestellt werden. Abbruch.")
        return False
    
    success = True
    
    # Synchronisiere Aufgaben
    if os.getenv("NOTION_DB_AUFGABEN"):
        aufgaben = get_all_aufgaben()
        if aufgaben:
            success = speichere_aufgaben(aufgaben) and success
    
    # Synchronisiere Offerten
    if os.getenv("NOTION_DB_OFFERTEN"):
        offerten = get_all_offerten()
        if offerten:
            success = speichere_offerten(offerten) and success
    
    # Synchronisiere Meetings
    if os.getenv("NOTION_DB_MEETINGS"):
        meetings = get_all_meetings()
        if meetings:
            success = speichere_meetings(meetings) and success
    
    # Synchronisiere Rechnungen
    if os.getenv("NOTION_DB_RECHNUNGEN"):
        rechnungen = get_all_rechnungen()
        if rechnungen:
            success = speichere_rechnungen(rechnungen) and success
    
    logger.info(f"Notion-Supabase Synchronisierung abgeschlossen. Erfolg: {success}")
    return success

if __name__ == "__main__":
    try:
        success = run_sync()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Unerwarteter Fehler bei der Synchronisierung: {e}")
        sys.exit(1)
