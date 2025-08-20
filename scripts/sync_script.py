import os
import sys
import time
import json
from dotenv import load_dotenv
from notion_client import Client
from supabase import create_client

# Logging-Setup mit Zeitstempel
def log(message):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# Lade Umgebungsvariablen
load_dotenv('.well-known/.env')

# Hole Umgebungsvariablen
notion_token = os.environ.get('NOTION_TOKEN')
db_aufgaben_id = os.environ.get('NOTION_DB_AUFGABEN')
db_raeume_id = os.environ.get('NOTION_DB_RAEUME')
supabase_url = os.environ.get('SUPABASE_URL')
supabase_key = os.environ.get('SUPABASE_KEY')

log(f"Verwende folgende IDs:")
log(f"- Aufgaben DB: {db_aufgaben_id}")
log(f"- Räume DB: {db_raeume_id}")
log(f"- Supabase URL: {supabase_url}")

# Initialisiere Clients
log("Initialisiere Clients...")
notion = Client(auth=notion_token)
supabase = create_client(supabase_url, supabase_key)

# Teste Supabase-Schreibzugriff
log("Teste Supabase-Schreibzugriff...")
try:
    test_uuid = f"test-uuid-{time.strftime('%Y%m%d-%H%M%S')}"
    test_data = {
        "notion_id": test_uuid,
        "name": "Testdaten",
        "status": "Test",
        "beschreibung": "Testbeschreibung"
    }
    result = supabase.table("aufgaben").upsert(test_data).execute()
    log(f"Test-Schreibzugriff erfolgreich: {len(result.data)} Datensätze eingefügt")
    
    # Bestätige, dass die Daten geschrieben wurden
    check = supabase.table("aufgaben").select("*").eq("notion_id", test_uuid).execute()
    if check.data:
        log(f"Test-Daten erfolgreich in Datenbank gefunden: {check.data}")
    else:
        log(f"⚠ WARNUNG: Test-Daten wurden nicht in der Datenbank gefunden!")
        
    # Lösche Testdaten wieder
    supabase.table("aufgaben").delete().eq("notion_id", test_uuid).execute()
    log("Test-Daten gelöscht")
    
except Exception as e:
    log(f"❌ FEHLER beim Test-Schreibzugriff: {str(e)}")
    import traceback
    log(traceback.format_exc())

def extract_title(prop):
    """Extrahiert den Titel aus einem Notion-Property"""
    try:
        if not prop:
            return ""
        # Title-Format
        if "title" in prop:
            titles = prop.get("title", [])
            if not titles:
                return ""
            return " ".join([t.get("plain_text", "") for t in titles])
        # Relation-Format (für db_Räume)
        elif "relation" in prop:
            log(f"Relation gefunden: {json.dumps(prop)}")
            relation_ids = prop.get("relation", [])
            if not relation_ids:
                return ""
            # IDs aus der Relation extrahieren
            relation_id_list = [rel_id.get("id", "") for rel_id in relation_ids]
            return json.dumps(relation_id_list)
        # Fallback
        return ""
    except Exception as e:
        log(f"Fehler beim Extrahieren eines Titels: {e}")
        return ""

def extract_text(prop):
    """Extrahiert Text aus einem Notion-Property"""
    try:
        if not prop:
            return ""
        # Rich Text Format
        if "rich_text" in prop:
            texts = prop.get("rich_text", [])
            if not texts:
                return ""
            return " ".join([t.get("plain_text", "") for t in texts])
        # Select Format
        elif "select" in prop and prop["select"]:
            return prop["select"].get("name", "")
        # Relation Format
        elif "relation" in prop:
            return json.dumps(prop["relation"])
        # Fallback
        return str(prop)
    except Exception as e:
        log(f"Fehler beim Extrahieren von Text: {e}")
        return ""

def extract_select(prop):
    """Extrahiert einen Select-Wert aus einem Notion-Property"""
    try:
        if not prop or "select" not in prop or not prop["select"]:
            return ""
        return prop["select"].get("name", "")
    except Exception as e:
        log(f"Fehler beim Extrahieren eines Select-Werts: {e}")
        return ""

# Verbesserte Relation-Extraktion für db_Räume
def extract_relation_data(relation_property, target_db_id=None):
    """Extrahiert Daten aus einer Relation und führt gegebenenfalls einen Lookup durch"""
    try:
        if not relation_property or "relation" not in relation_property:
            return []
        relation_ids = relation_property.get("relation", [])
        if not relation_ids:
            return []
        # Wenn keine target DB angegeben, geben wir nur die IDs zurück
        if not target_db_id:
            return [rel.get("id", "") for rel in relation_ids]
        # Für jeden verknüpften Eintrag Daten abrufen
        related_data = []
        for rel in relation_ids:
            rel_id = rel.get("id", "")
            if not rel_id:
                continue
            try:
                # Versuche, die verknüpften Daten abzurufen
                page_data = notion.pages.retrieve(page_id=rel_id)
                if page_data:
                    props = page_data.get("properties", {})
                    title = extract_title(props.get("Name", {}))
                    related_data.append({
                        "id": rel_id,
                        "title": title
                    })
            except Exception as inner_e:
                log(f"Fehler beim Abrufen von verknüpften Daten: {inner_e}")
        
        return related_data
        
    except Exception as e:
        log(f"Fehler beim Extrahieren von Relation-Daten: {e}")
        return []

def sync_aufgaben():
    """Synchronisiert Aufgaben von Notion zu Supabase"""
    log("Synchronisiere Aufgaben von Notion zu Supabase...")
    try:
        # Notion-Daten abrufen
        log(f"Rufe Aufgaben aus Notion-Datenbank ab: {db_aufgaben_id}")
        response = notion.databases.query(
            database_id=db_aufgaben_id,
            sorts=[{"property": "Name", "direction": "ascending"}]
        )
        results = response.get("results", [])
        log(f"Erfolgreich {len(results)} Aufgaben von Notion abgerufen")
        
        # Log erste Aufgabe zur Struktur-Überprüfung
        if results:
            log("Beispiel einer Aufgabe (ersten 1000 Zeichen):")
            sample_json = json.dumps(results[0], indent=2)[:1000]
            log(sample_json + ("..." if len(json.dumps(results[0])) > 1000 else ""))
            # Überprüfe verfügbare Properties
            props = results[0].get("properties", {})
            log(f"Verfügbare Properties in der ersten Aufgabe: {list(props.keys())}")
        
        # Aufgaben verarbeiten
        successful_syncs = 0
        for i, page in enumerate(results):
            try:
                page_id = page["id"]
                page_url = page.get("url", "")
                props = page.get("properties", {})
                last_edited = page.get("last_edited_time", "")
                
                # Extrahiere Aufgabendaten für notion_aufgaben Tabelle
                notion_aufgabe_data = {
                    "id": page_id,
                    "url": page_url,
                    "last_edited_time": last_edited,
                    "Name": extract_title(props.get("Name", {})),
                    "Bezeichnung": extract_text(props.get("Beschreibung", {})),
                    "ID_Aufgaben": page_id,
                    "notion_id": page_id,
                    "raw_data": json.dumps(page),
                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Extrahiere Aufgabendaten für aufgaben Tabelle
                aufgabe_data = {
                    "name": extract_title(props.get("Name", {})),
                    "status": extract_select(props.get("Status", {})),
                    "beschreibung": extract_text(props.get("Beschreibung", {})),
                    "notion_id": page_id,
                    "raum_ids": extract_relation_data(props.get("Raum", {}) or props.get("Verknüpft mit db_Räume (Db_Aufgaben)", {}))
                }
                
                # Ausgabe für die ersten 3 zur Überprüfung
                if i < 3:
                    log(f"Aufgabe {i+1}: {notion_aufgabe_data['Name']} ({aufgabe_data['status']})")
                    log(f"  - notion_aufgaben Daten: {json.dumps(notion_aufgabe_data)[:300]}...")
                    log(f"  - aufgaben Daten: {json.dumps(aufgabe_data)}")
                
                # In Supabase speichern (notion_aufgaben)
                result1 = supabase.table("notion_aufgaben").upsert(notion_aufgabe_data).execute()
                
                # In Supabase speichern (aufgaben)
                result2 = supabase.table("aufgaben").upsert(aufgabe_data, on_conflict="notion_id").execute()
                
                # Überprüfen, ob beide Upserts erfolgreich waren
                if result1.data and result2.data:
                    successful_syncs += 1
                    if i < 3:
                        log("  - ✓ Erfolgreich in beiden Tabellen gespeichert")
                else:
                    if i < 3:
                        log(f"  - ⚠ Unvollständige Speicherung: notion_aufgaben={bool(result1.data)}, aufgaben={bool(result2.data)}")
            
            except Exception as e:
                log(f"❌ Fehler bei Aufgabe {i+1}: {str(e)}")
                import traceback
                log(traceback.format_exc())
        
        log(f"Aufgaben-Synchronisation abgeschlossen: {successful_syncs} von {len(results)} Aufgaben erfolgreich synchronisiert")
        
        try:
            notion_aufgaben_data = supabase.table("notion_aufgaben").select("id").execute()
            notion_aufgaben_count = len(notion_aufgaben_data.data) if hasattr(notion_aufgaben_data, 'data') else 0
            aufgaben_data = supabase.table("aufgaben").select("notion_id").execute()
            aufgaben_count = len(aufgaben_data.data) if hasattr(aufgaben_data, 'data') else 0
            log(f"Anzahl der Datensätze in 'notion_aufgaben': {notion_aufgaben_count}")
            log(f"Anzahl der Datensätze in 'aufgaben': {aufgaben_count}")
        except Exception as e:
            log(f"Fehler beim Abfragen der Datensatzanzahl: {str(e)}")
        
        return successful_syncs > 0
        
    except Exception as e:
        log(f"❌ Fehler bei der Aufgaben-Synchronisation: {str(e)}")
        import traceback
        log(traceback.format_exc())
        return False

def sync_raeume():
    """Synchronisiert Räume von Notion zu Supabase"""
    log("Synchronisiere Räume von Notion zu Supabase...")
    try:
        log(f"Rufe Räume aus Notion-Datenbank ab: {db_raeume_id}")
        response = notion.databases.query(
            database_id=db_raeume_id
        )
        results = response.get("results", [])
        log(f"Erfolgreich {len(results)} Räume von Notion abgerufen")
        
        if results:
            log("Beispiel eines Raums (ersten 1000 Zeichen):")
            sample_json = json.dumps(results[0], indent=2)[:1000]
            log(sample_json + ("..." if len(json.dumps(results[0])) > 1000 else ""))
            props = results[0].get("properties", {})
            log(f"Verfügbare Properties im ersten Raum: {list(props.keys())}")
        
        successful_syncs = 0
        for i, page in enumerate(results):
            try:
                page_id = page["id"]
                page_url = page.get("url", "")
                props = page.get("properties", {})
                last_edited = page.get("last_edited_time", "")
                
                # Versuche verschiedene mögliche Property-Namen für Name/Location
                location = extract_title(props.get("Name", {}) or props.get("Location", {}) or props.get("Raum", {}) or props.get("Raum1", {}))
                
                # Extrahiere Raumdaten für notion_raeume Tabelle
                notion_raum_data = {
                    "id": page_id,
                    "url": page_url,
                    "created_time": page.get("created_time", ""),
                    "last_edited_time": last_edited,
                    "Location": location,
                    "Standort": extract_select(props.get("Standort", {})),
                    "notion_id": page_id,
                    "raw_data": json.dumps(page),
                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Extrahiere db_Räume-Verknüpfungen, falls vorhanden
                db_raum_properties = [
                    "db_Räume", "DB_Räume", "Db_Räume", "DB_Raume", "Db_Raume", 
                    "db_raeume", "DB_raeume", "Db_raeume"
                ]
                
                for prop_name in db_raum_properties:
                    if prop_name in props:
                        db_raeume_relation = props.get(prop_name, {})
                        related_ids = extract_relation_data(db_raeume_relation)
                        if related_ids:
                            for related_id in related_ids:
                                bruecke_data = {
                                    "raum_id": page_id,
                                    "db_raum_id": related_id,
                                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                                try:
                                    supabase.table("bruecke_raum").upsert(bruecke_data).execute()
                                    if i < 3:
                                        log(f"  - ✓ Verknüpfung mit db_Raum {related_id} gespeichert")
                                except Exception as inner_e:
                                    log(f"  - ⚠ Fehler beim Speichern der Verknüpfung: {str(inner_e)}")
                
                if i < 3:
                    log(f"Raum {i+1}: {notion_raum_data['Location']} ({notion_raum_data['Standort']})")
                    log(f"  - notion_raeume Daten: {json.dumps(notion_raum_data)[:300]}...")
                    log(f"  - Original Properties: {list(props.keys())}")
                
                result = supabase.table("notion_raeume").upsert(notion_raum_data).execute()
                
                if result.data:
                    successful_syncs += 1
                    if i < 3:
                        log("  - ✓ Erfolgreich in notion_raeume gespeichert")
                else:
                    log(f"  - ⚠ Keine Bestätigung von Supabase für Raum {i+1}")
            
            except Exception as e:
                log(f"❌ Fehler bei Raum {i+1}: {str(e)}")
                import traceback
                log(traceback.format_exc())
        
        log(f"Raum-Synchronisation abgeschlossen: {successful_syncs} von {len(results)} Räume erfolgreich synchronisiert")
        
        try:
            notion_raeume_data = supabase.table("notion_raeume").select("id").execute()
            notion_raeume_count = len(notion_raeume_data.data) if hasattr(notion_raeume_data, 'data') else 0
            bruecke_raum_data = supabase.table("bruecke_raum").select("raum_id").execute()
            bruecke_raum_count = len(bruecke_raum_data.data) if hasattr(bruecke_raum_data, 'data') else 0
            log(f"Finale Anzahl der Datensätze in 'notion_raeume': {notion_raeume_count}")
            log(f"Finale Anzahl der Datensätze in 'bruecke_raum': {bruecke_raum_count}")
        except Exception as e:
            log(f"Fehler beim Abfragen der Datensatzanzahl: {str(e)}")
        
        return successful_syncs > 0
    
    except Exception as e:
        log(f"❌ Fehler bei der Raum-Synchronisation: {str(e)}")
        import traceback
        log(traceback.format_exc())
        return False

def ensure_table_column(table_name, column_name, column_type):
    """Stellt sicher, dass eine Spalte in einer Tabelle existiert"""
    try:
        result = supabase.rpc(
            'check_column_exists',
            {'p_table': table_name, 'p_column': column_name}
        ).execute()
        if result.data and not result.data[0]:
            log(f"Spalte '{column_name}' existiert nicht in Tabelle '{table_name}'. Versuche zu erstellen...")
            supabase.rpc(
                'add_column',
                {'p_table': table_name, 'p_column': column_name, 'p_type': column_type}
            ).execute()
            log(f"✓ Spalte '{column_name}' in Tabelle '{table_name}' erstellt")
    except Exception as e:
        log(f"Fehler beim Überprüfen/Erstellen der Spalte '{column_name}' in Tabelle '{table_name}': {str(e)}")

def update_aufgaben_raeume_verknuepfungen():
    """Aktualisiert die Verknüpfungen zwischen Aufgaben und Räumen"""
    log("Aktualisiere Aufgaben-Räume-Verknüpfungen...")
    try:
        # 1. Zuerst versuchen, die RPC-Funktion aufzurufen, falls sie existiert
        try:
            log("Versuche RPC-Funktion 'refresh_aufgaben_raeume_view' aufzurufen...")
            refresh_result = supabase.rpc('refresh_aufgaben_raeume_view').execute()
            log("✓ View-Aktualisierungsfunktion erfolgreich aufgerufen")
        except Exception as rpc_error:
            log(f"ℹ RPC-Funktion nicht verfügbar: {str(rpc_error)}")
            # 2. Falls die Funktion nicht existiert, direkte SQL-Abfrage versuchen
            try:
                log("Versuche SQL-Abfrage zum Aktualisieren der View...")
                sql_query = """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'view_aufgaben_mit_raeumen'
                    ) THEN
                        REFRESH MATERIALIZED VIEW public.view_aufgaben_mit_raeumen;
                        RAISE NOTICE 'Materialized view refreshed';
                    ELSE
                        RAISE NOTICE 'No materialized view to refresh';
                    END IF;
                END
                $$;
                """
                sql_result = supabase.sql(sql_query).execute()
                log("✓ SQL-Abfrage zum Aktualisieren der View ausgeführt")
            except Exception as sql_error:
                log(f"ℹ SQL-Abfrage nicht möglich: {str(sql_error)}")
                log("Überprüfe View-Existenz durch Abfrage...")
        
        # Am Ende überprüfen, ob die View existiert und Daten enthält
        try:
            view_data = supabase.table("view_aufgaben_mit_raeumen").select("id,aufgaben,raeume").limit(5).execute()
            view_count = len(view_data.data) if hasattr(view_data, 'data') else 0
            if view_count > 0:
                log(f"✓ View enthält Daten (Beispiel-IDs: {[item.get('id', 'n/a') for item in view_data.data[:3]]})")
                log(f"  Beispieldaten: {view_data.data[0] if view_data.data else 'Keine Daten'}")
            else:
                log("ℹ View existiert, enthält aber möglicherweise keine Daten")
        except Exception as view_error:
            log(f"ℹ View konnte nicht abgefragt werden: {str(view_error)}")
            log("Die View 'view_aufgaben_mit_raeumen' scheint nicht korrekt konfiguriert zu sein.")
        
        return True
    except Exception as e:
        log(f"⚠ Fehler beim Verknüpfen von Aufgaben und Räumen: {str(e)}")
        return True

# Führe die Synchronisationen durch
log("Starte Synchronisationsprozess...")
success_aufgaben = sync_aufgaben()
success_raeume = sync_raeume()
update_erfolg = update_aufgaben_raeume_verknuepfungen()
log("Überprüfe nach der Synchronisation die Anzahl der Datensätze in Supabase...")

try:
    try:
        notion_aufgaben_data = supabase.table("notion_aufgaben").select("id").execute()
        notion_aufgaben_count = len(notion_aufgaben_data.data) if hasattr(notion_aufgaben_data, 'data') else 0
        log(f"Finale Anzahl der Datensätze in 'notion_aufgaben': {notion_aufgaben_count}")
    except Exception as e:
        log(f"Fehler beim Abfragen von 'notion_aufgaben': {str(e)}")
    
    try:
        aufgaben_data = supabase.table("aufgaben").select("notion_id").execute()
        aufgaben_count = len(aufgaben_data.data) if hasattr(aufgaben_data, 'data') else 0
        log(f"Finale Anzahl der Datensätze in 'aufgaben': {aufgaben_count}")
    except Exception as e:
        log(f"Fehler beim Abfragen von 'aufgaben': {str(e)}")
    
    try:
        notion_raeume_data = supabase.table("notion_raeume").select("id").execute()
        notion_raeume_count = len(notion_raeume_data.data) if hasattr(notion_raeume_data, 'data') else 0
        log(f"Finale Anzahl der Datensätze in 'notion_raeume': {notion_raeume_count}")
    except Exception as e:
        log(f"Fehler beim Abfragen von 'notion_raeume': {str(e)}")
    
    try:
        bruecke_raum_data = supabase.table("bruecke_raum").select("raum_id").execute()
        bruecke_raum_count = len(bruecke_raum_data.data) if hasattr(bruecke_raum_data, 'data') else 0
        log(f"Finale Anzahl der Datensätze in 'bruecke_raum': {bruecke_raum_count}")
    except Exception as e:
        log(f"Fehler beim Abfragen von 'bruecke_raum': {str(e)}")
    
    try:
        view_data = supabase.table("view_aufgaben_mit_raeumen").select("*").execute()
        view_count = len(view_data.data) if hasattr(view_data, 'data') else 0
        log(f"Finale Anzahl der Datensätze in der View: {view_count}")
    except Exception as e:
        log(f"View 'view_aufgaben_mit_raeumen' konnte nicht abgefragt werden: {str(e)}")
except Exception as e:
    log(f"Fehler bei der abschließenden Prüfung: {str(e)}")

if success_aufgaben and success_raeume:
    log("✅ Alle Synchronisationen erfolgreich abgeschlossen!")
    sys.exit(0)
elif success_aufgaben:
    log("⚠ Nur Aufgaben-Synchronisation erfolgreich.")
    sys.exit(0)
elif success_raeume:
    log("⚠ Nur Raum-Synchronisation erfolgreich.")
    sys.exit(0)
else:
    log("❌ Keine Synchronisation erfolgreich.")
    sys.exit(1)