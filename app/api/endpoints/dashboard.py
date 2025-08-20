from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
from app.services.supabase import get_supabase_client

router = APIRouter()

@router.get("/")
async def get_dashboard_data(
    zeitraum: Optional[str] = Query("monat", description="Zeitraum für die Dashboard-Daten (tag, woche, monat, jahr)"),
    standort: Optional[str] = Query(None, description="Filter nach Standort")
):
    try:
        # Hole den Supabase-Client
        supabase = get_supabase_client()
        if not supabase:
            raise HTTPException(status_code=500, detail="Keine Verbindung zu Supabase")
        
        # Sammle Daten für das Dashboard
        dashboard_data = {
            "aufgaben": get_dashboard_aufgaben(supabase, zeitraum, standort),
            "offerten": get_dashboard_offerten(supabase, zeitraum, standort),
            "meetings": get_dashboard_meetings(supabase, zeitraum, standort),
            "rechnungen": get_dashboard_rechnungen(supabase, zeitraum, standort)
        }
        
        return {
            "data": dashboard_data,
            "message": "Dashboard-Daten erfolgreich abgerufen"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fehler beim Abrufen der Dashboard-Daten: {str(e)}")

# Hilfsfunktionen zum Sammeln der Dashboard-Daten
def get_dashboard_aufgaben(supabase, zeitraum, standort=None):
    try:
        query = supabase.table("aufgaben").select("status, count")
        
        if standort:
            query = query.eq("standort", standort)
            
        # Hier könntest du weitere Filterungen basierend auf dem Zeitraum hinzufügen
        
        response = query.execute()
        
        # Umwandlung in ein leicht zu verarbeitendes Format
        result = {
            "offen": 0,
            "in_bearbeitung": 0,
            "erledigt": 0,
            "total": 0
        }
        
        if response and response.data:
            for item in response.data:
                status = item.get("status", "").lower()
                count = item.get("count", 1)
                
                if status == "offen":
                    result["offen"] += count
                elif status == "in bearbeitung":
                    result["in_bearbeitung"] += count
                elif status == "erledigt":
                    result["erledigt"] += count
                
                result["total"] += count
        
        return result
    except Exception:
        # Im Fehlerfall ein leeres Ergebnis zurückgeben
        return {
            "offen": 0,
            "in_bearbeitung": 0,
            "erledigt": 0,
            "total": 0
        }

def get_dashboard_offerten(supabase, zeitraum, standort=None):
    # Ähnlich wie get_dashboard_aufgaben implementieren
    return {
        "offen": 0,
        "angenommen": 0,
        "abgelehnt": 0,
        "total": 0
    }

def get_dashboard_meetings(supabase, zeitraum, standort=None):
    # Ähnlich implementieren
    return {
        "geplant": 0,
        "abgeschlossen": 0,
        "total": 0
    }

def get_dashboard_rechnungen(supabase, zeitraum, standort=None):
    # Ähnlich implementieren
    return {
        "offen": 0,
        "bezahlt": 0,
        "ueberfaellig": 0,
        "total": 0,
        "summe_offen": 0,
        "summe_bezahlt": 0
    }
