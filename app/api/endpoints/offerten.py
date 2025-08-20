from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
from app.services.supabase import get_offerten_from_supabase, get_supabase_client

router = APIRouter()

@router.get("/")
async def get_offerten(
    status: Optional[str] = Query(None, description="Filter nach Status"),
    standort: Optional[str] = Query(None, description="Filter nach Standort"),
    kunde_id: Optional[str] = Query(None, description="Filter nach Kunden-ID")
):
    try:
        offerten = get_offerten_from_supabase(status, standort, kunde_id)
        return {
            "data": offerten,
            "count": len(offerten),
            "message": "Offerten erfolgreich abgerufen"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Abrufen der Offerten: {str(e)}")

@router.get("/{offerte_id}")
async def get_offerte(offerte_id: str):
    try:
        # Hole den Supabase-Client
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Keine Verbindung zu Supabase")
        
        # Versuche die Offerte in verschiedenen Tabellen zu finden
        for table in ["offerten", "notion_offerten"]:
            try:
                response = supabase.table(table).select("*").eq("id", offerte_id).execute()
                if response.data and len(response.data) > 0:
                    return {
                        "data": response.data[0],
                        "message": "Offerte erfolgreich abgerufen"
                    }
            except Exception:
                continue
        
        # Wenn keine Offerte gefunden wurde
        raise HTTPException(status_code=404, detail=f"Offerte mit ID {offerte_id} nicht gefunden")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Abrufen der Offerte: {str(e)}")
