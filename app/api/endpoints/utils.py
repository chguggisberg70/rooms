from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def root():
    return {"message": "API ist online"}

@router.get("/health")
async def health():
    return {"status": "ok", "timestamp": "2025-03-23T14:30:00Z"}
