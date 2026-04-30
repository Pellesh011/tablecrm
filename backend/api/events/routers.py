from api.events.schemas import GetEvents
from fastapi import APIRouter, HTTPException, Query
from functions.events import get_events

router = APIRouter(prefix="/events", tags=["events"])


@router.get("/{token}/", response_model=GetEvents)
async def get_events_route(token: str, limit: int = Query(10), offset: int = Query(0)):
    events, count = await get_events(token=token, limit=limit, offset=offset)

    if not events:
        raise HTTPException(
            status_code=204, detail="События не найдены или токен не действителен"
        )
        # return "События не найдены или токен не действителен"

    return {"result": events, "count": count.count_1}
