import logging
from fastapi.routing import APIRoute
from fastapi import Request, Response

def setup_logging():
    """Konfiguriert das Logging-System."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

class LoggingRoute(APIRoute):
    """Route-Klasse, die Requests/Responses loggt."""
    def get_route_handler(self):
        original_route_handler = super().get_route_handler()
        
        async def custom_route_handler(request: Request) -> Response:
            logging.info(f"Request: {request.method} {request.url}")
            
            try:
                response = await original_route_handler(request)
                return response
            except Exception as e:
                logging.error(f"Error processing request: {e}")
                raise
                
        return custom_route_handler
