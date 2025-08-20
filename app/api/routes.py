from fastapi import FastAPI
from app.api.endpoints import (
    aufgaben, meetings, rechnungen, documents, openai, utils, offerten
)
from app.api.endpoints.openai import rag_search_root, OpenAIRequest

def setup_routes(app: FastAPI):
    """Registriert alle API-Routen."""
    
    # Root-Endpunkt
    app.include_router(utils.router)
    
    # RAG-Endpunkt auf Root-Ebene hinzufügen für Kompatibilität mit openapi-simple.yaml
    app.post("/rag")(rag_search_root)
    
    # Resource-Endpunkte
    app.include_router(
        aufgaben.router,
        prefix="/aufgaben",
        tags=["Aufgaben"]
    )
    
    app.include_router(
        meetings.router,
        prefix="/meetings", 
        tags=["Meetings"]
    )
    
    app.include_router(
        rechnungen.router,
        prefix="/rechnungen", 
        tags=["Rechnungen"]
    )
    
    app.include_router(
        offerten.router,
        prefix="/offerten", 
        tags=["Offerten"]
    )
    
    app.include_router(
        documents.router,
        prefix="/documents", 
        tags=["Dokumente"]
    )
    
    app.include_router(
        openai.router,
        prefix="/openai", 
        tags=["OpenAI"]
    )
