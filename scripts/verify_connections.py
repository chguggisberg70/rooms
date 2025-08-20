import os
import sys
from dotenv import load_dotenv
from notion_client import Client
from supabase import create_client

# Lade Umgebungsvariablen
load_dotenv('.well-known/.env')

def format_notion_id(database_id):
    """Formatiert eine Notion-ID für die API-Verwendung."""
    if not database_id:
        return None
    
    # Entferne alle vorhandenen Bindestriche
    clean_id = database_id.replace("-", "")
    
    # Wenn die ID bereits das richtige Format hat (36 Zeichen mit Bindestrichen)
    if len(database_id) == 36 and database_id.count("-") == 4:
        return database_id
        
    # Wenn es sich um eine "reine" ID ohne Bindestriche handelt (32 Zeichen)
    if len(clean_id) == 32:
        # Füge Bindestriche nach dem UUID-Schema ein: 8-4-4-4-12
        formatted_id = f"{clean_id[0:8]}-{clean_id[8:12]}-{clean_id[12:16]}-{clean_id[16:20]}-{clean_id[20:32]}"
        return formatted_id
    
    # Falls es ein anderes Format ist, gib die Originalversion zurück
    return database_id

# Prüfe Notion
notion_token = os.environ.get('NOTION_TOKEN')
print(f"Notion Token vorhanden: {'Ja' if notion_token else 'Nein'}")

try:
    notion = Client(auth=notion_token)
    user = notion.users.me()
    print(f"✓ Notion-Verbindung erfolgreich! Benutzer: {user.get('name')}")
    
    # Prüfe Notion-Datenbanken
    notion_dbs = {
        'NOTION_DB_AUFGABEN': os.environ.get('NOTION_DB_AUFGABEN'),
        'NOTION_DB_RAEUME': os.environ.get('NOTION_DB_RAEUME'),
        'NOTION_DB_BRUECKE': os.environ.get('NOTION_DB_BRUECKE'),
        'NOTION_DB_OFFERTEN': os.environ.get('NOTION_DB_OFFERTEN'),
        'NOTION_DB_RECHNUNGEN': os.environ.get('NOTION_DB_RECHNUNGEN'),
        'NOTION_DB_MEETINGS': os.environ.get('NOTION_DB_MEETINGS')
    }
    
    for db_name, db_id in notion_dbs.items():
        if db_id:
            try:
                # Formatiere die ID richtig
                formatted_id = format_notion_id(db_id)
                # Nur testen, ob die DB existiert, ohne alle Daten abzurufen
                notion.databases.retrieve(formatted_id)
                print(f"✓ Notion-Datenbank {db_name} erreichbar")
            except Exception as e:
                print(f"✗ Notion-Datenbank {db_name} nicht erreichbar: {e}")
        else:
            print(f"⚠ Notion-Datenbank {db_name} ID nicht konfiguriert")
    
except Exception as e:
    print(f"✗ Notion-Verbindung fehlgeschlagen: {e}")
    
# Prüfe Supabase
supabase_url = os.environ.get('SUPABASE_URL')
supabase_key = os.environ.get('SUPABASE_KEY')
print(f"Supabase URL: {supabase_url}")
print(f"Supabase Key vorhanden: {'Ja' if supabase_key else 'Nein'}")

try:
    supabase = create_client(supabase_url, supabase_key)
    response = supabase.table('aufgaben').select('*').limit(1).execute()
    print(f"✓ Supabase-Verbindung erfolgreich! Tabellen sind erreichbar.")
    
    # Tabellen-Struktur überprüfen
    tables = [
        'aufgaben', 
        'notion_aufgaben', 
        'notion_raeume', 
        'bruecke_raum',
        'notion_offerten',
        'notion_rechnungen',
        'notion_meetings'
    ]
    
    for table in tables:
        try:
            cols = supabase.table(table).select('*').limit(1).execute()
            print(f"✓ Tabelle '{table}' ist verfügbar mit Spalten: {list(cols.data[0].keys()) if cols.data else 'Keine Daten'}")
        except Exception as e:
            print(f"⚠ Tabelle '{table}' nicht verfügbar oder leer: {e}")
    
except Exception as e:
    print(f"✗ Supabase-Verbindung fehlgeschlagen: {e}")
