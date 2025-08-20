import logging
from typing import Dict, Any, Optional
from datetime import datetime
from app.services.supabase import get_supabase_client

logger = logging.getLogger(__name__)

def store_rag_query(
    query: str, 
    response: str, 
    has_context: bool = False, 
    user_id: Optional[str] = None, 
    rating: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Speichert eine RAG-Anfrage und ihre Antwort in der Datenbank.
    
    Args:
        query: Die Benutzeranfrage
        response: Die generierte Antwort
        has_context: Ob die Antwort auf Grundlage von Kontextdaten generiert wurde
        user_id: Optionale Benutzer-ID
        rating: Optionale Bewertung der Antwort (1-5)
        metadata: Zusätzliche Metadaten (z.B. verwendete Chunks, Tokens)
        
    Returns:
        Das gespeicherte Query-Objekt oder None bei einem Fehler
    """
    try:
        supabase = get_supabase_client()
        if not supabase:
            logger.error("Keine Verbindung zu Supabase.")
            return None
            
        logger.info(f"Speichere RAG-Anfrage: '{query[:50]}...'")
        
        # Daten für die Speicherung vorbereiten
        now = datetime.now().isoformat()
        query_data = {
            "query": query,
            "response": response,
            "has_context": has_context,
            "user_id": user_id or "anonymous",
            "rating": rating,
            "metadata": metadata or {},
            "created_at": now
        }
        
        # In die Datenbank einfügen
        # Hinweis: Die Tabelle 'rag_queries' muss in Supabase existieren
        response = supabase.table("rag_queries").insert(query_data).execute()
        
        if not response.data:
            logger.error("Keine Daten von Supabase erhalten beim Speichern der RAG-Anfrage")
            return None
        
        logger.info(f"RAG-Anfrage erfolgreich gespeichert mit ID {response.data[0].get('id')}")
        return response.data[0]
        
    except Exception as e:
        logger.error(f"Fehler beim Speichern der RAG-Anfrage: {e}")
        return None

def rate_rag_query(query_id: str, rating: int) -> bool:
    """
    Bewertet eine bestehende RAG-Anfrage.
    
    Args:
        query_id: ID der RAG-Anfrage
        rating: Bewertung (1-5)
        
    Returns:
        True bei Erfolg, False bei Fehler
    """
    try:
        if not 1 <= rating <= 5:
            logger.warning(f"Ungültige Bewertung: {rating}. Bewertung muss zwischen 1 und 5 liegen.")
            return False
            
        supabase = get_supabase_client()
        if not supabase:
            logger.error("Keine Verbindung zu Supabase.")
            return False
            
        logger.info(f"Bewerte RAG-Anfrage {query_id} mit {rating} Sternen")
        
        # In der Datenbank aktualisieren
        response = supabase.table("rag_queries").update({"rating": rating}).eq("id", query_id).execute()
        
        if not response.data:
            logger.error(f"Keine Daten von Supabase erhalten beim Bewerten der RAG-Anfrage {query_id}")
            return False
        
        logger.info(f"Bewertung für RAG-Anfrage {query_id} erfolgreich aktualisiert")
        return True
        
    except Exception as e:
        logger.error(f"Fehler beim Bewerten der RAG-Anfrage: {e}")
        return False

def get_similar_queries(query: str, limit: int = 5) -> list:
    """
    Findet ähnliche, bereits beantwortete Anfragen in der Datenbank.
    Diese Funktion verwendet eine einfache Textsuche (keine Embeddings).
    
    Args:
        query: Die Benutzeranfrage
        limit: Maximale Anzahl zurückzugebender Anfragen
        
    Returns:
        Liste ähnlicher Anfragen mit Antworten
    """
    try:
        supabase = get_supabase_client()
        if not supabase:
            logger.error("Keine Verbindung zu Supabase.")
            return []
            
        logger.info(f"Suche ähnliche Anfragen zu: '{query[:50]}...'")
        
        # Einfache Textsuche in der Datenbank
        # Diese könnte durch eine Embedding-basierte Suche ersetzt werden
        keywords = query.lower().split()
        results = []
        
        for keyword in keywords:
            if len(keyword) < 3:
                continue  # Zu kurze Keywords überspringen
                
            response = supabase.table("rag_queries").select("*").ilike("query", f"%{keyword}%").limit(limit).execute()
            
            if response.data:
                # Doppelte Einträge filtern
                for item in response.data:
                    if item not in results:
                        results.append(item)
                        
                # Sobald wir genug Ergebnisse haben, beenden
                if len(results) >= limit:
                    break
        
        logger.info(f"Gefunden: {len(results)} ähnliche Anfragen")
        return results[:limit]  # Begrenze auf gewünschte Anzahl
        
    except Exception as e:
        logger.error(f"Fehler beim Suchen ähnlicher Anfragen: {e}")
        return []

def get_top_rated_queries(min_rating: int = 4, limit: int = 10) -> list:
    """
    Ruft die am besten bewerteten RAG-Anfragen ab.
    
    Args:
        min_rating: Minimale Bewertung (1-5)
        limit: Maximale Anzahl zurückzugebender Anfragen
        
    Returns:
        Liste der am besten bewerteten Anfragen
    """
    try:
        supabase = get_supabase_client()
        if not supabase:
            logger.error("Keine Verbindung zu Supabase.")
            return []
            
        logger.info(f"Rufe bestbewertete RAG-Anfragen ab (Mindestbewertung: {min_rating})")
        
        response = supabase.table("rag_queries").select("*").gte("rating", min_rating).order("rating", desc=True).limit(limit).execute()
        
        logger.info(f"Erfolgreich {len(response.data)} bestbewertete Anfragen abgerufen")
        return response.data
        
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der bestbewerteten Anfragen: {e}")
        return []
