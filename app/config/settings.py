from pydantic_settings import BaseSettings
from typing import Optional, List
import os  # Dieser Import fehlt
from dotenv import load_dotenv
# Suche nach .env-Datei an verschiedenen möglichen Orten
possible_env_paths = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '.well-known', '.env'),  # Vom aktuellen Verzeichnis aus
    os.path.join(os.getcwd(), '.well-known', '.env'),  # Vom Arbeitsverzeichnis aus
    '.well-known/.env'  # Relativer Pfad
]

env_path = None
for path in possible_env_paths:
    if os.path.exists(path):
        env_path = path
        break

if env_path:
    load_dotenv(env_path)
    print(f"Umgebungsvariablen aus {env_path} geladen")
else:
    print("Keine .env-Datei gefunden!")
load_dotenv(env_path)


class Settings(BaseSettings):
    # App-Metadaten
    APP_TITLE: str = "Aufgaben Manager API"
    APP_DESCRIPTION: str = "API für Aufgaben, Meetings, Offerten und Rechnungen"
    APP_VERSION: str = "1.0.0"
    
    # Bereits definierte Einstellungen
    DEBUG: bool = False
    PORT: int = int(os.getenv("PORT", 8000))
    
    # CORS-Einstellungen
    CORS_ORIGINS: List[str] = ["*"]
    
    # Supabase-Einstellungen
    SUPABASE_URL: str
    SUPABASE_KEY: str
    
    # OpenAI-Einstellungen
    OPENAI_API_KEY: str
    
    # Notion-Einstellungen
    NOTION_TOKEN: str
    NOTION_DB_AUFGABEN: Optional[str] = None
    NOTION_DB_RAEUME: Optional[str] = None
    NOTION_DB_BRUECKE: Optional[str] = None
    NOTION_DB_RECHNUNGEN: Optional[str] = None
    NOTION_DB_OFFERTEN: Optional[str] = None
    NOTION_DB_MEETINGS: Optional[str] = None
    
    # Andere Einstellungen
    GitHubIntegrationToken: Optional[str] = None
    
    class Config:
        env_file = env_path
        env_file_encoding = 'utf-8'
        extra = "ignore"
# Debug-Ausgabe vor der Initialisierung
print(f"Prüfe Umgebungsvariablen vor dem Laden von Settings:")
for key in ["SUPABASE_URL", "SUPABASE_KEY", "OPENAI_API_KEY", "NOTION_TOKEN"]:
    print(f"{key} vorhanden: {'Ja' if os.getenv(key) else 'Nein'}")

settings = Settings()
