import os
import logging
import requests
import json
from notion_client import Client as NotionClient

# Logging konfigurieren
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Script-Verzeichnis zum Pfad hinzufügen
script_dir = os.path.dirname(os.path.abspath(__file__))

# Notion-Token aus Umgebungsvariablen laden
try:
    from dotenv import load_dotenv
    env_path = os.path.join(script_dir, '.well-known', '.env')
    load_dotenv(env_path)
    logger.info(f"Umgebungsvariablen aus {env_path} geladen")
except Exception as e:
    logger.error(f"Fehler beim Laden der Umgebungsvariablen: {e}")

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
if not NOTION_TOKEN:
    logger.warning("NOTION_TOKEN ist nicht in den Umgebungsvariablen gesetzt!")

notion = NotionClient(auth=NOTION_TOKEN) if NOTION_TOKEN else None

# Notion-Datenbank-IDs
NOTION_DB_AUFGABEN = os.getenv("NOTION_DB_AUFGABEN")
NOTION_DB_OFFERTEN = os.getenv("NOTION_DB_OFFERTEN")
NOTION_DB_RECHNUNGEN = os.getenv("NOTION_DB_RECHNUNGEN")
NOTION_DB_MEETINGS = os.getenv("NOTION_DB_MEETINGS")

# Logging der Datenbank-IDs
logger.info(f"NOTION_DB_AUFGABEN: {NOTION_DB_AUFGABEN}")
logger.info(f"NOTION_DB_OFFERTEN: {NOTION_DB_OFFERTEN}")
logger.info(f"NOTION_DB_RECHNUNGEN: {NOTION_DB_RECHNUNGEN}")
logger.info(f"NOTION_DB_MEETINGS: {NOTION_DB_MEETINGS}")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def format_notion_id(database_id):
    """Formatiert eine Notion-ID für die API-Verwendung.
    Entfernt alle vorhandenen Bindestriche und fügt sie an den richtigen Stellen ein,
    falls die ID nicht bereits im korrekten UUID-Format vorliegt.
    """
    if not database_id:
        logger.warning(f"Keine Datenbank-ID angegeben")
        return None
    
    # Entferne alle vorhandenen Bindestriche
    clean_id = database_id.replace("-", "")
    
    # Wenn die ID bereits das richtige Format hat (36 Zeichen mit Bindestrichen)
    if len(database_id) == 36 and database_id.count("-") == 4:
        logger.debug(f"ID hat bereits korrektes Format: {database_id}")
        return database_id
        
    # Wenn es sich um eine "reine" ID ohne Bindestriche handelt (32 Zeichen)
    if len(clean_id) == 32:
        # Füge Bindestriche nach dem UUID-Schema ein: 8-4-4-4-12
        formatted_id = f"{clean_id[0:8]}-{clean_id[8:12]}-{clean_id[12:16]}-{clean_id[16:20]}-{clean_id[20:32]}"
        logger.debug(f"ID formatiert: {database_id} -> {formatted_id}")
        return formatted_id
    
    # Falls es ein anderes Format ist, gib die Originalversion zurück
    logger.warning(f"Ungewöhnliches ID-Format: {database_id}")
    return database_id

def get_notion_data(database_id):
    """Holt alle Einträge aus einer Notion-Datenbank."""
    if not database_id:
        logger.error("Keine Datenbank-ID angegeben")
        return []
        
    # Formatiere die ID für die API-Anfrage
    formatted_id = format_notion_id(database_id)
    logger.info(f"Hole Daten aus Notion-Datenbank: {formatted_id}")
    
    url = f"https://api.notion.com/v1/databases/{formatted_id}/query"
    try:
        response = requests.post(url, headers=HEADERS)
        if response.status_code == 200:
            results = response.json().get("results", [])
            logger.info(f"Erfolgreich {len(results)} Einträge abgerufen")
            return results
        else:
            logger.error(f"Fehler beim Abrufen der Notion-Datenbank {database_id}: Status {response.status_code}")
            logger.error(f"Antwort: {response.text}")
            return []
    except Exception as e:
        logger.error(f"Exception beim Abrufen der Notion-Datenbank {database_id}: {e}")
        return []

def safe_extract_text(properties, field_name):
    """Extrahiert sicher einen Text aus einem title oder rich_text Feld."""
    if not field_name in properties:
        logger.debug(f"Feld '{field_name}' nicht in properties gefunden")
        return ""
        
    field = properties.get(field_name, {})
    field_type = field.get("type", "")
    
    if field_type == "title" or field_type == "rich_text":
        items = field.get(field_type, [])
        if items and len(items) > 0:
            return items[0].get("text", {}).get("content", "")
    elif field_type == "status":
        return field.get("status", {}).get("name", "")
    elif field_type == "date":
        return field.get("date", {}).get("start", "")
    elif field_type == "number":
        return field.get("number", 0)
        
    logger.debug(f"Konnte Text aus Feld '{field_name}' nicht extrahieren (Typ: {field_type})")
    return ""

def extract_aufgaben(entries):
    """Extrahiert relevante Felder für Aufgaben."""
    logger.info(f"Extrahiere Daten aus {len(entries)} Aufgaben-Einträgen")
    extracted_data = []
    for i, entry in enumerate(entries):
        try:
            properties = entry.get("properties", {})
            
            # Debug-Ausgabe für den ersten Eintrag
            if i == 0:
                logger.debug(f"Erster Eintrag Properties: {json.dumps(properties, indent=2)}")
            
            # Sichere Extraktion von title-Feldern
            name = ""
            title_list = properties.get("Name", {}).get("title", [])
            if title_list and len(title_list) > 0:
                name = title_list[0].get("text", {}).get("content", "")
            
            data = {
                "id": entry.get("id", ""),
                "name": name,
                "status": properties.get("Status", {}).get("status", {}).get("name", ""),
                "fälligkeitsdatum": properties.get("Fälligkeitsdatum", {}).get("date", {}).get("start", ""),
            }
            
            if i == 0:
                logger.debug(f"Extrahierte Daten für ersten Eintrag: {data}")
                
            extracted_data.append(data)
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren von Aufgabe {i}: {e}")
            continue
    
    logger.info(f"Erfolgreich {len(extracted_data)} Aufgaben extrahiert")
    return extracted_data

def extract_offerten(entries):
    """Extrahiert relevante Felder für Offerten."""
    logger.info(f"Extrahiere Daten aus {len(entries)} Offerten-Einträgen")
    extracted_data = []
    for i, entry in enumerate(entries):
        try:
            properties = entry.get("properties", {})
            
            # Debug-Ausgabe für den ersten Eintrag
            if i == 0:
                logger.debug(f"Erster Offerten-Eintrag Properties: {json.dumps(properties, indent=2)}")
            
            # Sichere Extraktion von title und rich_text-Feldern
            name = ""
            title_list = properties.get("Offerte", {}).get("title", [])
            if title_list and len(title_list) > 0:
                name = title_list[0].get("text", {}).get("content", "")
            
            anbieter = ""
            anbieter_list = properties.get("Anbieter", {}).get("rich_text", [])
            if anbieter_list and len(anbieter_list) > 0:
                anbieter = anbieter_list[0].get("text", {}).get("content", "")
            
            beschreibung = ""
            beschreibung_list = properties.get("Beschreibung", {}).get("rich_text", [])
            if beschreibung_list and len(beschreibung_list) > 0:
                beschreibung = beschreibung_list[0].get("text", {}).get("content", "")
            
            data = {
                "id": entry.get("id", ""),
                "name": name,
                "anbieter": anbieter,
                "beschreibung": beschreibung,
                "betrag": properties.get("Betrag", {}).get("number", 0),
                "status": properties.get("Status of Offer", {}).get("status", {}).get("name", ""),
            }
            
            if i == 0:
                logger.debug(f"Extrahierte Daten für erste Offerte: {data}")
                
            extracted_data.append(data)
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren von Offerte {i}: {e}")
            continue
    
    logger.info(f"Erfolgreich {len(extracted_data)} Offerten extrahiert")
    return extracted_data

def extract_rechnungen(entries):
    """Extrahiert relevante Felder für Rechnungen."""
    logger.info(f"Extrahiere Daten aus {len(entries)} Rechnungen-Einträgen")
    extracted_data = []
    for i, entry in enumerate(entries):
        try:
            properties = entry.get("properties", {})
            
            # Debug-Ausgabe für den ersten Eintrag
            if i == 0:
                logger.debug(f"Erster Rechnungs-Eintrag Properties: {json.dumps(properties, indent=2)}")
            
            # Sichere Extraktion von title und rich_text-Feldern
            rechnung = ""
            title_list = properties.get("Rechnung", {}).get("title", [])
            if title_list and len(title_list) > 0:
                rechnung = title_list[0].get("text", {}).get("content", "")
            
            anbieter = ""
            anbieter_list = properties.get("Anbieter", {}).get("rich_text", [])
            if anbieter_list and len(anbieter_list) > 0:
                anbieter = anbieter_list[0].get("text", {}).get("content", "")
            
            data = {
                "id": entry.get("id", ""),
                "rechnung": rechnung,
                "betrag": properties.get("Rechnungsbetrag", {}).get("number", 0),
                "anbieter": anbieter,
                "status": properties.get("ZahlungStatus", {}).get("status", {}).get("name", ""),
            }
            
            if i == 0:
                logger.debug(f"Extrahierte Daten für erste Rechnung: {data}")
                
            extracted_data.append(data)
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren von Rechnung {i}: {e}")
            continue
    
    logger.info(f"Erfolgreich {len(extracted_data)} Rechnungen extrahiert")
    return extracted_data

def extract_meetings(entries):
    """Extrahiert relevante Felder für Meetings."""
    logger.info(f"Extrahiere Daten aus {len(entries)} Meeting-Einträgen")
    extracted_data = []
    for i, entry in enumerate(entries):
        try:
            properties = entry.get("properties", {})
            
            # Debug-Ausgabe für den ersten Eintrag
            if i == 0:
                logger.debug(f"Erster Meeting-Eintrag Properties: {json.dumps(properties, indent=2)}")
            
            # Sichere Extraktion von title und rich_text-Feldern
            titel = ""
            title_list = properties.get("Titel", {}).get("title", [])
            if title_list and len(title_list) > 0:
                titel = title_list[0].get("text", {}).get("content", "")
            
            # Extrahiere Teilnehmer - prüfe zuerst rich_text, dann multi_select
            teilnehmer = []
            if "Meeting_Teilnehmer" in properties:
                teilnehmer_field = properties.get("Meeting_Teilnehmer", {})
                field_type = teilnehmer_field.get("type", "")
                
                if field_type == "rich_text":
                    teilnehmer_list = teilnehmer_field.get("rich_text", [])
                    if teilnehmer_list and len(teilnehmer_list) > 0:
                        text = teilnehmer_list[0].get("text", {}).get("content", "")
                        if text:
                            # Split text by commas if it's in the format "Person1, Person2, Person3"
                            teilnehmer = [name.strip() for name in text.split(',')]
                elif field_type == "multi_select":
                    for p in teilnehmer_field.get("multi_select", []):
                        if p.get("name"):
                            teilnehmer.append(p.get("name"))
            
            # Prüfe verschiedene Agenda-Felder
            agenda = ""
            if "Agenda" in properties:
                agenda_field = properties.get("Agenda", {})
                field_type = agenda_field.get("type", "")
                
                if field_type == "rich_text":
                    agenda_list = agenda_field.get("rich_text", [])
                    if agenda_list and len(agenda_list) > 0:
                        agenda = agenda_list[0].get("text", {}).get("content", "")
                elif field_type == "number":
                    agenda = str(agenda_field.get("number", ""))
            
            # Prüfe Datum-Feld
            datum = ""
            if "Datum_Meeting" in properties:
                datum_field = properties.get("Datum_Meeting", {})
                if datum_field.get("type") == "date":
                    datum = datum_field.get("date", {}).get("start", "")
            
            data = {
                "id": entry.get("id", ""),
                "titel": titel,
                "datum": datum,
                "teilnehmer": teilnehmer,
                "agenda": agenda,
            }
            
            if i == 0:
                logger.debug(f"Extrahierte Daten für erstes Meeting: {data}")
                
            extracted_data.append(data)
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren von Meeting {i}: {e}")
            continue
    
    logger.info(f"Erfolgreich {len(extracted_data)} Meetings extrahiert")
    return extracted_data

def get_all_aufgaben():
    """Holt alle Aufgaben aus Notion."""
    logger.info("Hole alle Aufgaben aus Notion")
    return extract_aufgaben(get_notion_data(NOTION_DB_AUFGABEN))

def get_all_offerten():
    """Holt alle Offerten aus Notion."""
    logger.info("Hole alle Offerten aus Notion")
    if not NOTION_DB_OFFERTEN:
        logger.error("NOTION_DB_OFFERTEN ist nicht konfiguriert")
        return []
    return extract_offerten(get_notion_data(NOTION_DB_OFFERTEN))

def get_all_rechnungen():
    """Holt alle Rechnungen aus Notion."""
    logger.info("Hole alle Rechnungen aus Notion")
    if not NOTION_DB_RECHNUNGEN:
        logger.error("NOTION_DB_RECHNUNGEN ist nicht konfiguriert")
        return []
    return extract_rechnungen(get_notion_data(NOTION_DB_RECHNUNGEN))

def get_all_meetings():
    """Holt alle Meetings aus Notion."""
    logger.info("Hole alle Meetings aus Notion")
    if not NOTION_DB_MEETINGS:
        logger.error("NOTION_DB_MEETINGS ist nicht konfiguriert")
        return []
    return extract_meetings(get_notion_data(NOTION_DB_MEETINGS))

# Test-Funktion
def test_connection():
    """Testet die Verbindung zu Notion."""
    if not notion:
        logger.error("Notion-Client nicht initialisiert (kein Token)")
        return False
        
    try:
        user = notion.users.me()
        logger.info(f"Notion-Verbindung erfolgreich! Benutzer: {user.get('name')}")
        return True
    except Exception as e:
        logger.error(f"Notion-Verbindung fehlgeschlagen: {e}")
        return False

if __name__ == "__main__":
    # Teste die Verbindung
    if test_connection():
        # Hole Rohdaten zum Debuggen
        if NOTION_DB_OFFERTEN:
            raw_offerten_data = get_notion_data(NOTION_DB_OFFERTEN)
            print(f"Rohdaten von Offerten: {raw_offerten_data[:1]}")
        
        if NOTION_DB_MEETINGS:
            raw_meetings_data = get_notion_data(NOTION_DB_MEETINGS)
            print(f"Rohdaten von Meetings: {raw_meetings_data[:1]}")
        
        try:
            # Hole Beispieldaten
            aufgaben = get_all_aufgaben()
            logger.info(f"Anzahl Aufgaben: {len(aufgaben)}")
            if aufgaben:
                logger.info(f"Erste Aufgabe: {aufgaben[0]}")
            
            offerten = get_all_offerten()
            logger.info(f"Anzahl Offerten: {len(offerten)}")
            if offerten:
                logger.info(f"Erste Offerte: {offerten[0]}")
            
            rechnungen = get_all_rechnungen()
            logger.info(f"Anzahl Rechnungen: {len(rechnungen)}")
            if rechnungen:
                logger.info(f"Erste Rechnung: {rechnungen[0]}")
            
            meetings = get_all_meetings()
            logger.info(f"Anzahl Meetings: {len(meetings)}")
            if meetings:
                logger.info(f"Erstes Meeting: {meetings[0]}")
        except Exception as e:
            logger.error(f"Fehler beim Testen der Datenextraktion: {e}")
    else:
        logger.error("Test der Notion-Verbindung fehlgeschlagen!")
