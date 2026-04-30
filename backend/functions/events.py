from database.db import database, events
from sqlalchemy import desc, func, select
from ws_manager import manager


async def get_events(token: str, limit: int, offset: int):
    query_result = (
        events.select()
        .where(events.c.token == token)
        .limit(limit)
        .offset(offset)
        .order_by(desc(events.c.id))
    )
    query_count = select(func.count(events.c.id)).where(events.c.token == token)

    res = await database.fetch_all(query_result)
    count = await database.fetch_one(query_count)

    return res, count


async def write_event(**event):
    event_id = await database.execute(events.insert().values(**event))
    if event["token"]:
        await manager.send_message(
            event["token"],
            {"action": "create", "target": "events", "result": {**events}},
        )
    return event_id
