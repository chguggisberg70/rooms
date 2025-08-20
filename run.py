import uvicorn
import os
from app.config.settings import settings

print(f"Starting server with PORT={settings.PORT}")

if __name__ == "__main__":
    print("Running server directly")
    uvicorn.run("app.main:app", 
               host="0.0.0.0", 
               port=settings.PORT, 
               reload=settings.DEBUG)
else:
    print("Imported as a module, still running server")
    uvicorn.run("app.main:app", 
               host="0.0.0.0", 
               port=settings.PORT, 
               reload=False)  # In Produktionsumgebungen kein Reload
