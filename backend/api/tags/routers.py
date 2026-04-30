from typing import List

from api.tags import schemas
from asyncpg import UniqueViolationError
from database.db import database, tags
from fastapi import APIRouter, HTTPException
from functions.helpers import get_user_by_token
from sqlalchemy import and_
from starlette import status
from starlette.responses import Response

router = APIRouter(prefix="/tags", tags=["tags"])


@router.get("/", response_model=List[schemas.Tag])
async def get_tags(token: str):
    user = await get_user_by_token(token)
    query = tags.select().where(tags.c.cashbox_id == user.cashbox_id)
    rows = await database.fetch_all(query)
    return [
        schemas.Tag(
            id=row.id,
            name=row.name,
            color=row.color,
            emoji=row.emoji,
            description=row.description,
        )
        for row in rows
    ]


@router.get("/{idx}", response_model=schemas.Tag)
async def get_tags(idx: int, token: str):
    user = await get_user_by_token(token)
    query = tags.select().where(
        and_(tags.c.cashbox_id == user.cashbox_id, tags.c.id == idx)
    )
    tag = await database.fetch_one(query)
    if not tag:
        raise HTTPException(status_code=404, detail="Тег не найден")
    return schemas.Tag(
        id=tag.id,
        name=tag.name,
        color=tag.color,
        emoji=tag.emoji,
        description=tag.description,
    )


@router.post("/", response_model=schemas.Tag)
async def create_tag(token: str, data: schemas.TagCreate):
    user = await get_user_by_token(token)
    data = data.dict()
    try:
        query = tags.insert().values(**data, cashbox_id=user.cashbox_id)
        new_tag_id = await database.execute(query)
    except UniqueViolationError:
        raise HTTPException(
            status_code=400, detail="Тег с таким именем уже существует!"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail="Ошибка при создании сегмента!")
    if not new_tag_id:
        raise HTTPException(status_code=400, detail="Ошибка при создании сегмента!")
    return schemas.Tag(id=new_tag_id, **data)


@router.delete("/{idx}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_tag(idx: int, token: str):
    user = await get_user_by_token(token)
    query = (
        tags.delete()
        .where(and_(tags.c.cashbox_id == user.cashbox_id, tags.c.id == idx))
        .returning(tags.c.id)
    )
    deleted_id = await database.execute(query)
    if not deleted_id:
        raise HTTPException(status_code=404, detail="Тег не найден")
    return Response(status_code=status.HTTP_204_NO_CONTENT, content=None)


@router.put("/{idx}", response_model=schemas.Tag)
async def update_tag(idx: int, token: str, data: schemas.TagCreate):
    user = await get_user_by_token(token)
    try:
        query = (
            tags.update()
            .where(and_(tags.c.cashbox_id == user.cashbox_id, tags.c.id == idx))
            .values(**data.dict(), cashbox_id=user.cashbox_id)
        )
        await database.execute(query)
    except UniqueViolationError:
        raise HTTPException(
            status_code=400, detail="Тег с таким именем уже существует!"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail="Ошибка при обновлении сегмента!")
    query = tags.select().where(tags.c.id == idx)
    tag = await database.fetch_one(query)
    return schemas.Tag(
        id=tag.id,
        name=tag.name,
        color=tag.color,
        emoji=tag.emoji,
        description=tag.description,
    )
