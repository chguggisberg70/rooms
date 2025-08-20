from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter()

@router.get("/")
async def get_documents(
    limit: int = Query(50, description="Maximale Anzahl der Ergebnisse"),
    offset: int = Query(0, description="Anzahl zu überspringender Ergebnisse")
):
    return {
        "data": [],
        "count": 0,
        "message": "Dokumente-API wird implementiert"
    }

@router.get("/search")
async def search_documents(
    query: str = Query(..., description="Suchbegriff"),
    limit: int = Query(10, description="Maximale Anzahl der Ergebnisse")
):
    return {
        "results": [],
        "count": 0,
        "query": query
    }
