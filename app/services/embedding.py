import logging
from typing import List, Dict, Any, Optional
from openai import OpenAI
from app.config.settings import settings
from app.services.supabase import fetch_relevant_chunks

logger = logging.getLogger(__name__)

# Initialisiere OpenAI-Client für Embeddings
try:
    openai_client = OpenAI(api_key=settings.OPENAI_API_KEY)
    logger.info("OpenAI-Client für Embeddings erfolgreich initialisiert")
except Exception as e:
    logger.error(f"Fehler bei der Initialisierung des OpenAI-Clients für Embeddings: {e}")
    openai_client = None

def create_embedding(text: str) -> List[float]:
    """
    Erstellt ein Embedding für den gegebenen Text mit der OpenAI API.
    
    Args:
        text: Der Text, für den ein Embedding erstellt werden soll
        
    Returns:
        Liste von Floats, die das Embedding repräsentieren
        
    Raises:
        ValueError: Wenn der OpenAI-Client nicht initialisiert ist oder ein Fehler auftritt
    """
    try:
        if not openai_client:
            logger.error("OpenAI-Client nicht initialisiert.")
            raise ValueError("OpenAI-Client nicht verfügbar")
            
        logger.info(f"Erstelle Embedding für Text: '{text[:50]}...'")
        
        response = openai_client.embeddings.create(
            model="text-embedding-ada-002",
            input=[text]
        )
        
        embedding = response.data[0].embedding
        
        logger.info(f"Embedding erfolgreich erstellt, Dimensionen: {len(embedding)}")
        return embedding
        
    except Exception as e:
        logger.error(f"Fehler bei der Erstellung des Embeddings: {e}")
        raise ValueError(f"Fehler beim Erstellen des Embeddings: {str(e)}")

def get_context_for_query(query: str, max_chunks: int = 5) -> str:
    """
    Erstellt ein Embedding für die Query und ruft die relevantesten Dokument-Chunks ab.
    
    Args:
        query: Die Suchanfrage
        max_chunks: Maximale Anzahl der zurückzugebenden Chunks
        
    Returns:
        Kombinierter Text aus allen relevanten Chunks
    """
    try:
        logger.info(f"Suche relevanten Kontext für Query: '{query}'")
        
        # Embedding für die Query erstellen
        query_embedding = create_embedding(query)
        
        # Relevante Chunks aus der Datenbank abrufen
        chunks = fetch_relevant_chunks(query_embedding, limit=max_chunks)
        
        if not chunks:
            logger.warning("Keine relevanten Chunks gefunden")
            return ""
            
        # Alle Chunk-Texte extrahieren und kombinieren
        context_text = ""
        for i, chunk in enumerate(chunks):
            chunk_text = chunk.get("chunk_text", "")
            if chunk_text:
                context_text += f"Segment {i+1}:\n{chunk_text}\n\n"
        
        logger.info(f"Kontext erfolgreich zusammengestellt, Länge: {len(context_text)} Zeichen")
        return context_text.strip()
        
    except Exception as e:
        logger.error(f"Fehler beim Abrufen des Kontexts: {e}")
        return ""

def semantic_search(query: str, max_chunks: int = 5) -> List[Dict[str, Any]]:
    """
    Führt eine semantische Suche durch und gibt die relevantesten Chunks zurück.
    
    Args:
        query: Die Suchanfrage
        max_chunks: Maximale Anzahl der zurückzugebenden Chunks
        
    Returns:
        Liste von Chunks mit Text und Metadaten
    """
    try:
        logger.info(f"Führe semantische Suche durch für: '{query}'")
        
        # Embedding für die Query erstellen
        query_embedding = create_embedding(query)
        
        # Relevante Chunks aus der Datenbank abrufen
        chunks = fetch_relevant_chunks(query_embedding, limit=max_chunks)
        
        if not chunks:
            logger.warning("Keine relevanten Chunks gefunden")
            return []
            
        logger.info(f"Semantische Suche erfolgreich, {len(chunks)} Chunks gefunden")
        return chunks
        
    except Exception as e:
        logger.error(f"Fehler bei der semantischen Suche: {e}")
        return []
