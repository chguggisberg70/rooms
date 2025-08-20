import logging
from typing import List, Optional, Dict, Any
from openai import OpenAI
from app.config.settings import settings

logger = logging.getLogger(__name__)

# Initialisiere OpenAI-Client
try:
    openai_client = OpenAI(api_key=settings.OPENAI_API_KEY)
    logger.info("OpenAI-Client erfolgreich initialisiert")
except Exception as e:
    logger.error(f"Fehler bei der Initialisierung des OpenAI-Clients: {e}")
    openai_client = None

def get_openai_client():
    """Gibt den OpenAI-Client zurück."""
    return openai_client

def generate_gpt_response(prompt: str, context: Optional[str] = None, max_tokens: int = 500) -> Dict[str, Any]:
    """
    Generiert eine Antwort mit GPT basierend auf dem Prompt und optional dem Kontext.
    
    Args:
        prompt: Die Anfrage des Benutzers
        context: Optionaler Kontext aus relevanten Dokumenten
        max_tokens: Maximale Anzahl der Tokens in der Antwort
        
    Returns:
        Dict mit der generierten Antwort und Metadaten
    """
    try:
        if not openai_client:
            logger.error("OpenAI-Client nicht initialisiert.")
            return {"error": "OpenAI-Client nicht verfügbar", "response": ""}
            
        logger.info(f"Generiere GPT-Antwort für Prompt: '{prompt[:50]}...'")
        
        # System-Prompt für RAG erstellen, wenn Kontext vorhanden ist
        system_content = "Du bist ein hilfreicher Assistent."
        if context:
            system_content = """Du bist ein hilfreicher Assistent, der auf Basis der gegebenen Informationen antwortet.
            Verwende ausschließlich die bereitgestellten Informationen, um die Frage zu beantworten.
            Wenn die Informationen keine Antwort liefern, sage ehrlich, dass du es nicht weißt.
            Zitiere keine Quellen in deiner Antwort."""
            
            user_content = f"""Frage: {prompt}
            
            Kontext:
            {context}
            
            Bitte beantworte die Frage basierend auf dem gegebenen Kontext."""
        else:
            user_content = prompt
        
        # Generiere Antwort mit OpenAI
        response = openai_client.chat.completions.create(
            model="gpt-4",  # oder settings.OPENAI_MODEL für Flexibilität
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content}
            ],
            max_tokens=max_tokens,
            temperature=0.7
        )
        
        # Antworttext extrahieren
        response_text = response.choices[0].message.content
        
        logger.info(f"Antwort erfolgreich generiert: '{response_text[:50]}...'")
        return {
            "response": response_text,
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens
        }
        
    except Exception as e:
        logger.error(f"Fehler bei der Generierung der GPT-Antwort: {e}")
        return {
            "error": str(e),
            "response": "Es ist ein Fehler bei der Verarbeitung Ihrer Anfrage aufgetreten."
        }

def simple_chat(prompt: str, max_tokens: int = 150) -> Dict[str, Any]:
    """
    Einfache Chat-Anfrage ohne Kontext.
    
    Args:
        prompt: Die Anfrage des Benutzers
        max_tokens: Maximale Anzahl der Tokens in der Antwort
        
    Returns:
        Dict mit der generierten Antwort und Metadaten
    """
    try:
        if not openai_client:
            logger.error("OpenAI-Client nicht initialisiert.")
            return {"error": "OpenAI-Client nicht verfügbar", "response": ""}
            
        logger.info(f"Generiere einfache Chat-Antwort für: '{prompt[:50]}...'")
        
        # Generiere Antwort mit OpenAI
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",  # Schnelleres Modell für einfache Anfragen
            messages=[
                {"role": "user", "content": prompt}
            ],
            max_tokens=max_tokens,
            temperature=0.7
        )
        
        # Antworttext extrahieren
        response_text = response.choices[0].message.content
        
        logger.info(f"Chat-Antwort erfolgreich generiert")
        return {
            "response": response_text,
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens
        }
        
    except Exception as e:
        logger.error(f"Fehler bei der Generierung der Chat-Antwort: {e}")
        return {
            "error": str(e),
            "response": "Es ist ein Fehler bei der Verarbeitung Ihrer Anfrage aufgetreten."
        }
