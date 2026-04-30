from typing import List

from api.tags import schemas as tags_schemas
from database.db import database, segments, segments_tags, tags
from fastapi import APIRouter, Body, HTTPException, status
from functions.helpers import get_user_by_token
from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert

router = APIRouter(prefix="/segments/{idx}/tags", tags=["segmentsTags"])


async def _get_segment(idx: int, user):
    query = select(segments).where(
        segments.c.id == idx, segments.c.cashbox_id == user.cashbox_id
    )
    seg = await database.fetch_one(query)
    return seg


@router.get("/", response_model=List[tags_schemas.Tag])
async def list_segment_tags(idx: int, token: str):
    """Список тегов, привязанных к сегменту"""
    user = await get_user_by_token(token)
    segment = await _get_segment(idx, user)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")

    query = (
        select(tags.c.id, tags.c.name, tags.c.color, tags.c.emoji, tags.c.description)
        .select_from(tags.join(segments_tags, segments_tags.c.tag_id == tags.c.id))
        .where(
            and_(
                segments_tags.c.segment_id == idx, tags.c.cashbox_id == user.cashbox_id
            )
        )
    )
    rows = await database.fetch_all(query)
    return [
        tags_schemas.Tag(
            id=row.id,
            name=row.name,
            color=row.color,
            emoji=row.emoji,
            description=row.description,
        )
        for row in rows
    ]


@router.post(
    "/", response_model=List[tags_schemas.Tag], status_code=status.HTTP_201_CREATED
)
async def add_tags_to_segment(
    idx: int, token: str, tags_data: List[tags_schemas.TagCreate]
):
    """
    Добавляет теги к сегменту.
    Если тегов с такими именами нет — создаёт их (в рамках cashbox).
    Возвращает список привязанных к сегменту тегов.
    """
    user = await get_user_by_token(token)
    segment = await _get_segment(idx, user)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")

    # подготовим данные для вставки в tags (создаём отсутствующие)
    names = [t.name for t in tags_data]
    prepared = []
    for t in tags_data:
        prepared.append(
            {
                "name": t.name,
                "emoji": t.emoji,
                "color": t.color,
                "description": t.description,
                "cashbox_id": user.cashbox_id,
            }
        )

    if prepared:
        insert_tags_q = insert(tags).values(prepared)
        # уникальность по (name, cashbox_id) — в db определено
        insert_tags_q = insert_tags_q.on_conflict_do_nothing(
            index_elements=["name", "cashbox_id"]
        )
        await database.execute(insert_tags_q)

    # получаем id тегов
    sel_q = select(
        tags.c.id, tags.c.name, tags.c.color, tags.c.emoji, tags.c.description
    ).where(and_(tags.c.name.in_(names), tags.c.cashbox_id == user.cashbox_id))
    tag_rows = await database.fetch_all(sel_q)
    if not tag_rows:
        return []

    # привязываем теги к сегменту
    seg_values = []
    for row in tag_rows:
        seg_values.append(
            {"tag_id": row.id, "segment_id": idx, "cashbox_id": user.cashbox_id}
        )

    if seg_values:
        insert_seg_tags_q = insert(segments_tags).values(seg_values)
        insert_seg_tags_q = insert_seg_tags_q.on_conflict_do_nothing(
            index_elements=["tag_id", "segment_id"]
        )
        await database.execute(insert_seg_tags_q)

    # вернуть актуальный список привязанных тегов
    return await list_segment_tags(idx, token)


@router.delete("/", status_code=status.HTTP_204_NO_CONTENT)
async def remove_tags_from_segment(
    idx: int, token: str, tags_body: List[tags_schemas.TagDelete] = Body(...)
):
    """
    Удаляет теги из сегмента по именам, переданным в теле:
    [{ "name": "vip" }, { "name": "prospect" }]
    """
    user = await get_user_by_token(token)
    segment = await _get_segment(idx, user)
    if not segment:
        raise HTTPException(status_code=404, detail="Сегмент не найден")

    # извлекаем уникальные имена из тела
    del_names = list({t.name for t in tags_body if getattr(t, "name", None)})
    if not del_names:
        raise HTTPException(
            status_code=400, detail="Не переданы имена тегов для удаления"
        )

    # подзапрос: id тегов с такими именами в текущем cashbox
    tag_ids_subq = select(tags.c.id).where(
        and_(tags.c.name.in_(del_names), tags.c.cashbox_id == user.cashbox_id)
    )

    # удаляем записи из segments_tags, у которых tag_id входит в подзапрос и segment_id == idx
    del_q = segments_tags.delete().where(
        and_(
            segments_tags.c.segment_id == idx, segments_tags.c.tag_id.in_(tag_ids_subq)
        )
    )

    await database.execute(del_q)
    return
