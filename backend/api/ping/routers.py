from fastapi import APIRouter

router = APIRouter(tags=["ping"])


@router.get("/ping/")
async def ping_pong_func():
    return {"ping": "pong"}
