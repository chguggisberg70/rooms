from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import setup_routes
from app.services.supabase import init_supabase
from app.api.endpoints import aufgaben, meetings, rechnungen, offerten, dashboard
import logging

# Logger konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI-App erstellen
app = FastAPI(title="Projektverwaltung")

# CORS-Middleware für Frontend-Anfragen
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Statische Dateien (CSS, JavaScript, Bilder)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Supabase initialisieren
@app.on_event("startup")
async def startup_db_client():
    """Initialisiert die Datenbankverbindung beim Start der App."""
    init_supabase()
    logger.info("Verbindung zu Supabase initialisiert.")

# API-Routen einrichten
setup_routes(app)

# Frontend-Routen einrichten
app.include_router(dashboard.router)
app.include_router(aufgaben.router)
app.include_router(meetings.router)
app.include_router(rechnungen.router)
app.include_router(offerten.router)  # Füge Offerten-Router hinzu

# Root-Weiterleitung zum Dashboard
@app.get("/")
async def root():
    """Leitet von der Root-URL zum Dashboard weiter."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/dashboard")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
