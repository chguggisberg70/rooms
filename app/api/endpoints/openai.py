from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Dict, Any
from app.services.embedding import create_embedding, get_context_for_query, semantic_search
from app.services.openai import generate_gpt_response, simple_chat
from app.services.rag_queries import store_rag_query, rate_rag_query, get_similar_queries

router = APIRouter()

class OpenAIRequest(BaseModel):
    prompt: str
    max_tokens: Optional[int] = 500
    store_query: Optional[bool] = True
    user_id: Optional[str] = None

class RatingRequest(BaseModel):
    query_id: str
    rating: int  # 1-5

@router.post("/chat")
async def openai_chat(data: OpenAIRequest):
    """
    Generiert eine einfache Antwort mit GPT ohne Dokumentenkontext.
    """
    try:
        result = simple_chat(data.prompt, max_tokens=data.max_tokens)
        
        if "error" in result and result["error"]:
            raise HTTPException(status_code=500, detail=result["error"])
            
        # Speichere die Anfrage, wenn gewünscht
        if data.store_query:
            metadata = {
                "tokens": result.get("total_tokens", 0),
                "model": "gpt-3.5-turbo"
            }
            store_rag_query(
                query=data.prompt,
                response=result["response"],
                has_context=False,
                user_id=data.user_id,
                metadata=metadata
            )
            
        return {
            "reply": result["response"],
            "tokens": result.get("total_tokens", 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler bei der OpenAI-Anfrage: {str(e)}")

@router.post("/rag")
async def rag_search(data: OpenAIRequest):
    """
    Generiert eine Antwort basierend auf den relevanten Dokumenten im System.
    
    Dieser Endpunkt:
    1. Erstellt ein Embedding für den Prompt
    2. Sucht nach relevanten Dokumenten-Chunks in der Datenbank
    3. Nutzt die Chunks als Kontext für die GPT-Antwort
    4. Speichert die Anfrage und Antwort (optional)
    """
    try:
        # Prüfe zuerst, ob es ähnliche Anfragen gab
        similar_queries = get_similar_queries(data.prompt, limit=1)
        
        # Wenn eine sehr ähnliche Anfrage mit guter Bewertung existiert, verwende diese
        if similar_queries and similar_queries[0].get("rating", 0) >= 4:
            # Nutze die gespeicherte Antwort
            previous_query = similar_queries[0]
            
            return {
                "response": previous_query["response"],
                "tokens": previous_query.get("metadata", {}).get("tokens", 0),
                "has_context": previous_query.get("has_context", False),
                "from_cache": True,
                "query_id": previous_query.get("id")
            }
        
        # Relevanten Kontext aus der Datenbank abrufen
        context = get_context_for_query(data.prompt)
        has_context = bool(context)
        
        if not context:
            # Keine relevanten Dokumente gefunden, erstelle eine generische Antwort
            result = generate_gpt_response(
                f"Zu '{data.prompt}' konnte ich leider keine relevanten Informationen in der Datenbank finden. "
                f"Kannst du eine allgemeine Antwort geben und erklären, dass keine spezifischen Daten verfügbar sind?",
                context=None,
                max_tokens=data.max_tokens
            )
        else:
            # Generiere Antwort mit dem gefundenen Kontext
            result = generate_gpt_response(
                data.prompt,
                context=context,
                max_tokens=data.max_tokens
            )
        
        if "error" in result and result["error"]:
            raise HTTPException(status_code=500, detail=result["error"])
        
        # Speichere die Anfrage, wenn gewünscht
        query_id = None
        if data.store_query:
            metadata = {
                "tokens": result.get("total_tokens", 0),
                "model": "gpt-4",
                "context_length": len(context) if context else 0
            }
            stored_query = store_rag_query(
                query=data.prompt,
                response=result["response"],
                has_context=has_context,
                user_id=data.user_id,
                metadata=metadata
            )
            if stored_query:
                query_id = stored_query.get("id")
            
        return {
            "response": result["response"],
            "tokens": result.get("total_tokens", 0),
            "has_context": has_context,
            "from_cache": False,
            "query_id": query_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler bei der RAG-Anfrage: {str(e)}")

@router.post("/rate")
async def rate_query(data: RatingRequest):
    """
    Bewertet eine zuvor gestellte RAG-Anfrage.
    
    Diese Bewertungen können verwendet werden, um die Qualität der Antworten zu überwachen
    und ähnliche Anfragen in Zukunft besser zu beantworten.
    """
    try:
        success = rate_rag_query(data.query_id, data.rating)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Anfrage mit ID {data.query_id} nicht gefunden oder Bewertung ungültig")
            
        return {
            "message": f"Anfrage mit ID {data.query_id} erfolgreich mit {data.rating} Sternen bewertet"
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Bewerten der Anfrage: {str(e)}")

@router.get("/similar-queries")
async def find_similar_queries(
    query: str = Query(..., description="Suchanfrage"),
    limit: int = Query(5, description="Maximale Anzahl der Ergebnisse")
):
    """
    Sucht nach ähnlichen, bereits beantworteten Anfragen.
    """
    try:
        similar = get_similar_queries(query, limit=limit)
        
        return {
            "queries": similar,
            "count": len(similar)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler bei der Suche nach ähnlichen Anfragen: {str(e)}")

# Diese Funktion wird auch direkt auf Root-Ebene in app/api/routes.py verwendet
async def rag_search_root(data: OpenAIRequest):
    """
    Identische Implementierung wie rag_search, aber für die direkte Verwendung auf Root-Ebene.
    """
    return await rag_search(data)
