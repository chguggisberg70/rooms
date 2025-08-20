from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
from app.services.supabase import get_aufgaben_from_supabase

router = APIRouter()

@router.get("/")
async def get_aufgaben(
    status: Optional[str] = Query(None, description="Filter nach Status"),
    standort: Optional[str] = Query(None, description="Filter nach Standort")
):
    try:
        aufgaben = get_aufgaben_from_supabase(status, standort)
        return {
            "data": aufgaben,
            "count": len(aufgaben),
            "message": "Aufgaben erfolgreich abgerufen"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Abrufen der Aufgaben: {str(e)}")
