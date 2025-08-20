from fastapi import APIRouter, Query
from typing import Optional
from uuid import UUID

router = APIRouter()

@router.get("/")
async def get_rechnungen(
    status: Optional[str] = Query(None, description="Filter nach Status"),
    raum_id: Optional[UUID] = Query(None, description="Filter nach Raum-ID")
):
    return {
        "data": [],
        "count": 0,
        "message": "Rechnungen-API wird implementiert"
    }

@router.get("/{rechnung_id}")
async def get_rechnung(rechnung_id: str):
    return {
        "data": {"id": rechnung_id, "betrag": 100.0},
        "message": "Rechnung abgerufen"
    }
