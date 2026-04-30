# api/segments/routers.py

import asyncio
import json
from typing import List, Optional

from api.segments import schemas
from api.segments.common import generate_unix_salt_md5, get_segments_tags
from api.segments.schema_base import SegmentType
from api.tags import schemas as tags_schemas
from database.db import SegmentStatus, database, segments, segments_tags, tags
from fastapi import APIRouter, HTTPException, Response
from functions.helpers import deep_sanitize, get_user_by_token
from segments.main import Segments, update_segment_task
from sqlalchemy import func

router = APIRouter(tags=["segments"])


def _parse_auto_reply(raw) -> Optional[dict]:
    if not raw:
        return None
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return None
    return dict(raw)


@router.post("/segments/", response_model=schemas.Segment)
async def create_segments(token: str, segment_data: schemas.SegmentCreate):
    user = await get_user_by_token(token)
    data = segment_data.dict(exclude_none=True)
    criteria = data.get("criteria")
    if criteria is None:
        criteria = {}
    tags_db = []

    if segment_data.auto_reply:
        pass
    else:
        if not segment_data.criteria:
            raise HTTPException(400, "Standard segment requires criteria")

    if tags_ids := data.get("tags_ids"):
        tags_db = await database.fetch_all(
            tags.select().where(
                tags.c.id.in_(tags_ids), tags.c.cashbox_id == user.cashbox_id
            )
        )

        exists_tags_ids = [tag.id for tag in tags_db]
        not_exists_tags_ids = list(set(tags_ids) - set(exists_tags_ids))

        if not_exists_tags_ids:
            raise HTTPException(
                status_code=404,
                detail=f"Тегов с id {not_exists_tags_ids} не существует!",
            )

    query = segments.insert().values(
        name=segment_data.name,
        description=data.get("description"),
        criteria=criteria,
        actions=data.get("actions"),
        auto_reply=data.get("auto_reply"),
        hash_tag=generate_unix_salt_md5(),
        cashbox_id=user.cashbox_id,
        type_of_update=data.get("type_of_update"),
        update_settings=data.get("update_settings"),
        status=SegmentStatus.in_process.value,
        is_archived=data.get("is_archived"),
        type=segment_data.type,
    )

    new_segment_id = await database.execute(query)
    for tag in tags_db:
        await database.execute(
            segments_tags.insert().values(
                tag_id=tag.id, segment_id=new_segment_id, cashbox_id=user.cashbox_id
            )
        )

    asyncio.create_task(update_segment_task(new_segment_id))
    segment = await database.fetch_one(
        segments.select().where(segments.c.id == new_segment_id)
    )

    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        description=segment.description,
        tags=[tags_schemas.Tag(**tag) for tag in tags_db],
        criteria=json.loads(segment.criteria),
        actions=json.loads(segment.actions) if segment.actions else {},
        auto_reply=_parse_auto_reply(segment.auto_reply),
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        is_archived=segment.is_archived,
    )


@router.post("/segments/{idx}", response_model=schemas.Segment)
async def refresh_segments(idx: int, token: str):
    user = await get_user_by_token(token)
    query = segments.select().where(
        segments.c.id == idx, segments.c.cashbox_id == user.cashbox_id
    )
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    if segment.is_archived:
        raise HTTPException(status_code=403, detail="Сегмент заархивирован!")
    if segment.is_deleted:
        raise HTTPException(status_code=403, detail="Сегмент удален!")

    await database.execute(
        segments.update()
        .where(segments.c.id == segment.id)
        .values(status=SegmentStatus.in_process.value)
    )
    asyncio.create_task(update_segment_task(segment.id))
    segment = await database.fetch_one(segments.select().where(segments.c.id == idx))

    tags_db = await get_segments_tags(segment.id)

    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        description=segment.description,
        tags=[tags_schemas.Tag(**tag) for tag in tags_db],
        criteria=json.loads(segment.criteria),
        actions=json.loads(segment.actions) if segment.actions else {},
        auto_reply=_parse_auto_reply(segment.auto_reply),
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        is_archived=segment.is_archived,
    )


@router.get("/segments/{idx}", response_model=schemas.Segment)
async def get_segment(idx: int, token: str):
    user = await get_user_by_token(token)
    query = segments.select().where(
        segments.c.id == idx, segments.c.cashbox_id == user.cashbox_id
    )
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    if segment.is_archived:
        raise HTTPException(status_code=403, detail="Сегмент заархивирован!")
    if segment.is_deleted:
        raise HTTPException(status_code=403, detail="Сегмент удален!")

    tags_db = await get_segments_tags(segment.id)

    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        description=segment.description,
        tags=[tags_schemas.Tag(**tag) for tag in tags_db],
        criteria=json.loads(segment.criteria),
        actions=json.loads(segment.actions) if segment.actions else {},
        auto_reply=_parse_auto_reply(segment.auto_reply),
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        is_archived=segment.is_archived,
        selection_field=segment.selection_field,
    )


@router.put("/segments/{idx}", response_model=schemas.Segment)
async def update_segments(idx: int, token: str, segment_data: schemas.SegmentCreate):
    user = await get_user_by_token(token)
    query = segments.select().where(
        segments.c.id == idx, segments.c.cashbox_id == user.cashbox_id
    )
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    if segment.is_deleted:
        raise HTTPException(status_code=403, detail="Сегмент удален!")

    data = segment_data.dict(exclude_none=True)
    criteria = data.get("criteria")
    if criteria is None:
        criteria = {}
    if segment_data.auto_reply:
        pass
    else:
        if not segment_data.criteria:
            raise HTTPException(400, "Standard segment requires criteria")

    tags_collect: List[tags_schemas.Tag] = []
    tags_db = await get_segments_tags(segment.id)
    tags_ids = data.get("tags_ids", [])
    for tag_obj in tags_db:
        if tag_obj.id not in tags_ids:
            await database.execute(tags.delete().where(tags.c.id == tag_obj.id))
        else:
            tags_collect.append(tags_schemas.Tag(**tag_obj))

    query = (
        segments.update()
        .where(segments.c.id == idx)
        .values(
            name=segment_data.name,
            criteria=criteria,
            actions=data.get("actions"),
            auto_reply=data.get("auto_reply"),
            cashbox_id=user.cashbox_id,
            type_of_update=data.get("type_of_update"),
            update_settings=data.get("update_settings"),
            status=SegmentStatus.in_process.value,
            is_archived=data.get("is_archived"),
        )
    )
    await database.execute(query)
    asyncio.create_task(update_segment_task(idx))
    segment = await database.fetch_one(segments.select().where(segments.c.id == idx))
    return schemas.Segment(
        id=segment.id,
        name=segment.name,
        description=segment.description,
        tags=tags_collect,
        criteria=json.loads(segment.criteria),
        actions=json.loads(segment.actions),
        auto_reply=_parse_auto_reply(segment.auto_reply),
        updated_at=segment.updated_at,
        type_of_update=segment.type_of_update,
        update_settings=json.loads(segment.update_settings),
        status=segment.status,
        is_archived=segment.is_archived,
        selection_field=segment.selection_field,
    )


@router.delete("/segments/{idx}")
async def delete_segments(idx: int, token: str):
    user = await get_user_by_token(token)
    query = segments.select().where(
        segments.c.id == idx, segments.c.cashbox_id == user.cashbox_id
    )
    segment = await database.fetch_one(query)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")

    query = (
        segments.update()
        .where(segments.c.id == idx)
        .values(
            is_deleted=True,
            updated_at=func.now(),
        )
    )
    await database.execute(query)
    return Response(status_code=204)


@router.get("/segments/{idx}/result", response_model=schemas.SegmentData)
async def get_segment_data(idx: int, token: str):
    user = await get_user_by_token(token)
    segment = Segments(idx)
    await segment.async_init()
    if not segment.segment_obj or segment.segment_obj.cashbox_id != user.cashbox_id:
        raise HTTPException(status_code=404, detail="Сегмент не найден")
    if segment.segment_obj.is_deleted:
        raise HTTPException(status_code=403, detail="Сегмент удален!")

    contragents_data = await segment.collect_data()
    return schemas.SegmentData(
        id=segment.segment_id,
        updated_at=segment.segment_obj.updated_at,
        **contragents_data,
    )


@router.get("/segments/", response_model=List[schemas.SegmentWithContragents])
async def get_user_segments(token: str, is_archived: Optional[bool] = None):
    user = await get_user_by_token(token)

    query = segments.select().where(
        segments.c.cashbox_id == user.cashbox_id, segments.c.is_deleted.isnot(True)
    )
    if is_archived is not None:
        query = query.where(segments.c.is_archived == is_archived)

    rows = await database.fetch_all(query)
    result = []

    for row in rows:
        segment = Segments(row.id)
        await segment.async_init()

        tags_db = await get_segments_tags(row.id)

        if not segment.segment_obj or segment.segment_obj.cashbox_id != user.cashbox_id:
            raise HTTPException(status_code=404, detail="Сегмент не найден")
        contragents_data = await segment.collect_data()

        sanitized_criteria = json.loads(row.criteria)
        sanitized_actions = json.loads(row.actions) if row.actions else {}
        sanitized_update_settings = json.loads(row.update_settings)
        sanitized_auto_reply = _parse_auto_reply(row.auto_reply)

        sanitized_criteria = deep_sanitize(sanitized_criteria)
        sanitized_actions = deep_sanitize(sanitized_actions)
        sanitized_update_settings = deep_sanitize(sanitized_update_settings)

        result.append(
            schemas.SegmentWithContragents(
                id=row.id,
                name=row.name,
                description=row.description,
                tags=[tags_schemas.Tag(**tag) for tag in tags_db],
                criteria=sanitized_criteria,
                actions=sanitized_actions,
                auto_reply=sanitized_auto_reply,
                updated_at=row.updated_at,
                type_of_update=row.type_of_update,
                update_settings=sanitized_update_settings,
                status=row.status,
                is_archived=row.is_archived,
                selection_field=row.selection_field,
                contragents_count=len(contragents_data["contragents"]),
                added_contragents_count=len(contragents_data["added_contragents"]),
                deleted_contragents_count=len(contragents_data["deleted_contragents"]),
                entered_contragents_count=len(contragents_data["entered_contragents"]),
                exited_contragents_count=len(contragents_data["exited_contragents"]),
            )
        )

    return result


@router.get("/segments/market/", response_model=List[schemas.Segment])
async def get_market_segments():
    segments_query = segments.select().where(
        segments.c.is_deleted == False,
        segments.c.is_archived == False,
        segments.c.type == SegmentType.market.value,
    )

    segments_db = await database.fetch_all(segments_query)
    segments_result = [schemas.Segment(**segment) for segment in segments_db]

    return segments_result


@router.post("/segments/market/", response_model=schemas.Segment)
async def setup_market_segment(idx: int, token: str, is_archived: bool = True):
    user = await get_user_by_token(token)

    segment_query = segments.select().where(
        segments.c.id == idx,
        segments.c.is_deleted.isnot(True),
        segments.c.is_archived.isnot(True),
        segments.c.type == SegmentType.market.value,
    )

    segment_db = await database.fetch_one(segment_query)

    if not segment_db:
        raise HTTPException(status_code=404, detail="Сегмент не найден!")

    segment_dict = dict(segment_db)
    segment_dict.pop("id", None)
    segment_dict["cashbox_id"] = user.cashbox_id
    segment_dict["type"] = SegmentType.standard.value
    segment_dict["is_archived"] = is_archived
    segment_dict["hash_tag"] = generate_unix_salt_md5()

    stmt = segments.insert().values(**segment_dict)

    new_segment_id = await database.execute(stmt)
    new_segment_db = await database.fetch_one(
        segments.select().where(segments.c.id == new_segment_id)
    )

    return schemas.Segment(**new_segment_db)
