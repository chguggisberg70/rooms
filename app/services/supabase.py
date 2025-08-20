"""
Supabase Service für die App.
Stellt Verbindungen und Funktionen für Supabase bereit.
"""
import os
import logging
import traceback
from typing import List, Dict, Any, Optional, Union
from supabase import create_client, Client
from app.config.settings import settings

logger = logging.getLogger(__name__)

# Supabase Client initialisieren
try:
    supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    logger.info(f"Supabase-Client erfolgreich initialisiert mit URL: {settings.SUPABASE_URL[:20]}...")
except Exception as e:
    logger.error(f"Fehler bei der Initialisierung des Supabase-Clients: {e}")
    supabase = None

def get_supabase_client() -> Optional[Client]:
    """
    Gibt den Supabase-Client zurück, falls initialisiert.
    
    Returns:
        Der Supabase-Client oder None bei Fehler
    """
    if supabase is None:
        logger.error("Supabase-Client ist nicht initialisiert")
    return supabase

def test_connection() -> bool:
    """
    Testet die Verbindung zu Supabase.
    
    Returns:
        True bei erfolgreicher Verbindung, False bei Fehler
    """
    try:
        if not supabase:
            logger.error("Supabase-Client ist nicht initialisiert")
            return False
            
        # Versuche eine einfache Abfrage
        response = supabase.table('document_chunks').select('id').limit(1).execute()
        logger.info("Supabase-Verbindung erfolgreich getestet")
        return True
    except Exception as e:
        logger.error(f"Supabase-Verbindungstest fehlgeschlagen: {e}")
        return False

def get_documents(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Ruft Dokumente aus der documents-Tabelle ab.
    
    Args:
        limit: Maximale Anzahl abzurufender Dokumente
        offset: Offset für Paginierung
        
    Returns:
        Liste von Dokumenten
    """
    try:
        if not supabase:
            logger.error("Supabase-Client ist nicht initialisiert")
            return []
            
        logger.info(f"Rufe Dokumente ab (limit={limit}, offset={offset})")
        response = supabase.table("documents").select("*").limit(limit).offset(offset).execute()
        
        if not response.data:
            logger.info("Keine Dokumente gefunden")
            return []
            
        logger.info(f"Erfolgreich {len(response.data)} Dokumente abgerufen")
        return response.data
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Dokumente: {e}")
        return []

def search_documents(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Durchsucht Dokumente nach einem Suchbegriff.
    
    Args:
        query: Suchbegriff
        limit: Maximale Anzahl an Ergebnissen
        
    Returns:
        Liste gefundener Dokumente
    """
    try:
        if not supabase:
            logger.error("Supabase-Client ist nicht initialisiert")
            return []
            
        logger.info(f"Durchsuche Dokumente nach '{query}'")
        response = supabase.table("documents").select("*").ilike("content", f"%{query}%").limit(limit).execute()
        
        if not response.data:
            logger.info("Keine Dokumente gefunden")
            return []
            
        logger.info(f"Erfolgreich {len(response.data)} Dokumente gefunden")
        return response.data
    except Exception as e:
        logger.error(f"Fehler bei der Dokumentensuche: {e}")
        return []

def create_document(file_name: str, file_type: str, content: str) -> Optional[Dict[str, Any]]:
    """
    Erstellt ein neues Dokument in der Datenbank.
    
    Args:
        file_name: Name der Datei
        file_type: Dateityp (z.B. PDF, DOCX)
        content: Textinhalt des Dokuments
        
    Returns:
        Das erstellte Dokument oder None bei Fehler
    """
    try:
        if not supabase:
            logger.error("Supabase-Client ist nicht initialisiert")
            return None
            
        logger.info(f"Erstelle neues Dokument: {file_name}")
        
        document_data = {
            "file_name": file_name,
            "file_type": file_type,
            "content": content
        }
        
        response = supabase.table("documents").insert(document_data).execute()
        
        if not response.data:
            logger.error("Keine Daten von Supabase erhalten beim Erstellen des Dokuments")
            return None
            
        logger.info(f"Dokument erfolgreich erstellt mit ID {response.data[0].get('id')}")
        return response.data[0]
    except Exception as e:
        logger.error(f"Fehler beim Erstellen des Dokuments: {e}")
        return None

def create_document_chunks(document_id: str, chunks: List[Dict[str, Any]]) -> bool:
    """
    Erstellt Dokumentchunks für ein bestehendes Dokument.
    
    Args:
        document_id: ID des zugehörigen Dokuments
        chunks: Liste von Chunks mit Text und ggf. Embedding
        
    Returns:
        True bei Erfolg, False bei Fehler
    """
    try:
        if not supabase:
            logger.error("Supabase-Client ist nicht initialisiert")
            return False
            
        logger.info(f"Erstelle {len(chunks)} Chunks für Dokument {document_id}")
        
        # Chunks vorbereiten
        chunk_data = []
        for i, chunk in enumerate(chunks):
            chunk_data.append({
                "document_id": document_id,
                "chunk_index": i,
                "chunk_text": chunk["text"],
                "embedding": chunk.get("embedding")
            })
        
        # Chunks in Batches einfügen (max. 1000 pro Anfrage)
        batch_size = 100
        for i in range(0, len(chunk_data), batch_size):
            batch = chunk_data[i:i+batch_size]
            response = supabase.table("document_chunks").insert(batch).execute()
            if not response.data:
                logger.warning(f"Keine Daten von Supabase für Batch {i//batch_size+1}")
        
        logger.info(f"Alle Chunks für Dokument {document_id} erfolgreich erstellt")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der Dokumentchunks: {e}")
        return False

def fetch_relevant_chunks(embedding, query, limit=5):
    """
    Ruft relevante Chunks basierend auf Embedding-Ähnlichkeit ab.
    
    Args:
        embedding: Das Embedding-Vektor der Abfrage
        query: Die Textabfrage
        limit: Maximale Anzahl der zurückzugebenden Chunks
        
    Returns:
        Liste von Chunks mit Text und Metadaten
    """
    try:
        # Sichere Überprüfung der Eingabeparameter
        if embedding is None:
            logger.error("Embedding ist None, nicht verarbeitbar")
            return []
        
        if not isinstance(embedding, list):
            try:
                embedding = list(embedding)
                logger.info(f"Embedding zu Liste konvertiert, Länge: {len(embedding)}")
            except Exception as e:
                logger.error(f"Embedding konnte nicht zu Liste konvertiert werden: {e}")
                return []
        
        # Sichere Integer-Konvertierung für limit
        try:
            match_count = max(1, int(limit))
        except (TypeError, ValueError):
            match_count = 5
            logger.warning(f"limit '{limit}' nicht gültig, verwende Standardwert 5")
        
        # Versuche match_document_chunks RPC
        try:
            logger.info("Versuche match_document_chunks RPC-Funktion...")
            response = supabase.rpc(
                'match_document_chunks',
                {'query_embedding': embedding, 'match_count': match_count}
            ).execute()
            
            if response.data:
                logger.info(f"Gefundene relevante Chunks: {len(response.data)}")
                return response.data
            else:
                logger.warning("Keine relevanten Chunks gefunden über match_document_chunks")
        except Exception as e:
            logger.error(f"match_document_chunks RPC fehlgeschlagen: {e}")
        
        # Versuche match_chunks als Alternative
        try:
            logger.info("Versuche match_chunks als Alternative...")
            response = supabase.rpc(
                'match_chunks',
                {'query_embedding': embedding, 'match_count': match_count}
            ).execute()
            
            if response.data:
                logger.info(f"Gefundene Chunks mit match_chunks: {len(response.data)}")
                return response.data
            else:
                logger.warning("Keine relevanten Chunks gefunden über match_chunks")
        except Exception as e:
            logger.error(f"match_chunks RPC fehlgeschlagen: {e}")
        
        # Fallback zu einfacher Textsuche
        try:
            logger.info("Verwende einfache Textsuche als Fallback...")
            query_text = query.lower() if isinstance(query, str) else str(query).lower()
            response = supabase.table("document_chunks").select(
                "id, chunk_text, document_id, chunk_index"
            ).ilike("chunk_text", f"%{query_text}%").limit(match_count).execute()
            
            if response.data:
                logger.info(f"Gefundene Chunks über Textsuche: {len(response.data)}")
                return response.data
            else:
                logger.warning("Keine relevanten Chunks über Textsuche gefunden")
                
                # Als letzte Option: einfach die neuesten Chunks zurückgeben
                logger.info("Gebe die neuesten Chunks zurück als letzte Option...")
                response = supabase.table("document_chunks").select(
                    "id, chunk_text, document_id, chunk_index"
                ).order("id", desc=True).limit(match_count).execute()
                
                if response.data:
                    logger.info(f"Stattdessen {len(response.data)} neueste Chunks zurückgegeben")
                    return response.data
        except Exception as e:
            logger.error(f"Textsuche fehlgeschlagen: {e}")
        
        logger.warning("Keine relevanten Chunks gefunden mit allen Methoden")
        return []
        
    except Exception as e:
        logger.error(f"Allgemeiner Fehler in fetch_relevant_chunks: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

def get_aufgaben(status: Optional[str] = None, standort: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Ruft Aufgaben mit optionaler Filterung ab.
    
    Args:
        status: Optionaler Status-Filter
        standort: Optionaler Standort-Filter
        
    Returns:
        Liste von Aufgaben
    """
    try:
        if not supabase:
            logger.error("Supabase-Client ist nicht initialisiert")
            return []
            
        logger.info(f"Rufe Aufgaben ab (Status={status}, Standort={standort})")
        
        # View für kombinierte Aufgaben-Informationen abfragen
        query = supabase.table("view_aufgaben_mit_raeumen").select("*")
        
        # Filter hinzufügen
        if status:
            status_lower = status.lower()
            # Mapping für verschiedene Status-Bezeichnungen
            status_map = {
                "offen": "in bearbeitung",
                "open": "in bearbeitung",
                "todo": "nicht begonnen",
                "done": "erledigt"
            }
            filter_status = status_map.get(status_lower, status_lower)
            query = query.eq("status", filter_status)
            
        if standort:
            query = query.eq("standorte", standort)
            
        # Daten abrufen
        response = query.execute()
        
        if not response.data:
            logger.info("Keine Aufgaben gefunden")
            return []
            
        logger.info(f"Erfolgreich {len(response.data)} Aufgaben abgerufen")
        return response.data
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Aufgaben: {e}")
        return []

def get_status_values() -> List[str]:
    """
    Ruft verfügbare Status-Werte für Aufgaben ab.
    
    Returns:
        Liste eindeutiger Status-Werte
    """
    try:
        if not supabase:
            logger.error("Supabase-Client ist nicht initialisiert")
            return []
            
        logger.info("Rufe verfügbare Status-Werte ab")
        response = supabase.table("view_aufgaben_mit_raeumen").select("status").execute()
        
        if not response.data:
            logger.info("Keine Status-Werte gefunden")
            return []
            
        # Einzigartige Status-Werte extrahieren
        status_values = set()
        for item in response.data:
            if status := item.get("status"):
                status_values.add(status)
                
        logger.info(f"Erfolgreich {len(status_values)} Status-Werte abgerufen")
        return sorted(list(status_values))
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Status-Werte: {e}")
        return []

def get_standorte() -> List[str]:
    """
    Ruft verfügbare Standorte für Aufgaben ab.
    
    Returns:
        Liste eindeutiger Standorte
    """
    try:
        if not supabase:
            logger.error("Supabase-Client ist nicht initialisiert")
            return []
            
        logger.info("Rufe verfügbare Standorte ab")
        response = supabase.table("view_aufgaben_mit_raeumen").select("standorte").execute()
        
        if not response.data:
            logger.info("Keine Standorte gefunden")
            return []
            
        # Einzigartige Standorte extrahieren
        standorte = set()
        for item in response.data:
            if standort := item.get("standorte"):
                standorte.add(standort)
                
        logger.info(f"Erfolgreich {len(standorte)} Standorte abgerufen")
        return sorted(list(standorte))
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Standorte: {e}")
        return []
