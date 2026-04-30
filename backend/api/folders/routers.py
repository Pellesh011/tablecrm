from typing import List

from api.folders import schemas
from api.folders.utils import (
    folder_exists,
    get_column_by_names,
    get_folders,
    get_table_by_name,
    item_exists,
    item_exists_in_folder,
)
from database.db import database, folders, folders_entities
from fastapi import APIRouter, HTTPException, Query, Response
from functions.helpers import get_user_by_token
from pydantic.fields import defaultdict
from sqlalchemy import insert, select, update

router = APIRouter(tags=["folders"])


@router.post("/folders/", response_model=schemas.Folder)
async def new_folder(token: str, folder_data: schemas.FolderCreate):
    user = await get_user_by_token(token)

    folder = folder_data.dict(exclude_none=True)
    folder["cashbox_id"] = user.cashbox_id
    folder["owner_id"] = user.id
    previous_folder_id = folder.get("previous_id")

    if previous_folder_id is not None:
        if not (await folder_exists(previous_folder_id, user.cashbox_id)):
            raise HTTPException(
                status_code=404, detail="Такой previous_folder не существует!"
            )

    smtp = insert(folders).values(**folder)
    new_folder_id = await database.execute(smtp)

    folder_db = await database.fetch_one(
        select(folders).where(folders.c.id == new_folder_id)
    )

    return schemas.Folder(**folder_db)


@router.delete("/folders/{folder_id}/")
async def delete_folder(idx: int, token: str):
    user = await get_user_by_token(token)

    if not (await folder_exists(idx, user.cashbox_id)):
        raise HTTPException(status_code=404, detail="Такой папки нет!")

    update_query = update(folders).where(folders.c.id == idx).values(is_deleted=True)
    update_entities_query = (
        update(folders_entities)
        .where(folders_entities.c.folder_id == idx)
        .values(is_deleted=True)
    )

    await database.execute(update_query)
    await database.execute(update_entities_query)

    return Response(status_code=204)


@router.put("/folders/{folder_id}/", response_model=schemas.Folder)
async def update_folder(idx: int, token: str, folder_data: schemas.FolderCreate):
    user = await get_user_by_token(token)
    folder = folder_data.dict(exclude_none=True)

    if not (await folder_exists(idx, user.cashbox_id)):
        raise HTTPException(status_code=404, detail="Такой папки нет!")

    update_query = update(folders).where(folders.c.id == idx).values(**folder)
    await database.execute(update_query)

    folder = (await get_folders(user.cashbox_id, [folders.c.id == idx]))[0]
    return schemas.Folder(**folder)


@router.get("/folders/{folder_id}/", response_model=schemas.Folder)
async def get_folder(token: str, is_active: bool, idx: int):
    user = await get_user_by_token(token)

    if not (await folder_exists(idx, user.cashbox_id)):
        raise HTTPException(status_code=404, detail="Папка не найдена!")

    folder_db = (
        await get_folders(
            user.cashbox_id, [folders.c.id == idx, folders.c.is_active == is_active]
        )
    )[0]

    return schemas.Folder(**folder_db)


@router.get("/folders/", response_model=schemas.FolderList)
async def get_user_folders(token: str):
    user = await get_user_by_token(token)
    folders_db = await get_folders(user.cashbox_id)

    return {
        "items": [schemas.Folder(**folder) for folder in folders_db],
        "count": len(folders_db),
    }


@router.get("/folders/{slug}", response_model=schemas.FolderList)
async def get_user_folders_by_slug(token: str, slug: str):
    user = await get_user_by_token(token)
    folders_db = await get_folders(user.cashbox_id, [folders.c.slug == slug])

    return {
        "items": [schemas.Folder(**folder) for folder in folders_db],
        "count": len(folders_db),
    }


@router.get(
    "/folders/{folder_id}/items/", response_model=List[schemas.FolderEntityList]
)
async def get_folder_items(
    token: str,
    idx: int,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
):
    user = await get_user_by_token(token)
    offset = (page - 1) * limit

    if not (await folder_exists(idx, user.cashbox_id)):
        raise HTTPException(status_code=404, detail="Папка не найдена!")

    entities_query = (
        select(folders_entities)
        .where(
            folders_entities.c.folder_id == idx,
            folders_entities.c.is_deleted == False,
        )
        .offset(offset)
        .limit(limit)
    )

    entities = await database.fetch_all(entities_query)
    entities_by_type = defaultdict(set)

    for entity in entities:
        entities_by_type[entity.get("entity_type")].add(entity.get("entity_id"))

    result = list()
    for type, ids in entities_by_type.items():
        table = get_table_by_name(type)
        entities = await database.fetch_all(select(table).where(table.c.id.in_(ids)))
        result.append(
            schemas.FolderEntityList(
                entity_type=type,
                entities_data=entities,
                count=len(entities),
            )
        )

    return result


@router.post("/folders/{folder_id}/items/", response_model=schemas.FolderEntityView)
async def add_folder_item(token: str, idx: int, item: schemas.FolderEntity):
    user = await get_user_by_token(token)
    item_dict = item.dict(exclude_none=True)

    if not (await item_exists(user.cashbox_id, item)):
        raise HTTPException(status_code=404, detail="Такой сущности не существует!")

    if await item_exists_in_folder(idx, item):
        raise HTTPException(status_code=409, detail="Этот объект уже есть в папке!")

    item_dict["folder_id"] = idx
    item_dict["owner_id"] = user.id

    insert_query = insert(folders_entities).values(**item_dict)
    await database.execute(insert_query)

    entity_table = get_table_by_name(item.entity_type)
    cashbox_col = get_column_by_names(entity_table, ["cashbox", "cashbox_id"])

    entity = await database.fetch_one(
        select(entity_table).where(
            entity_table.c.id == item.entity_id,
            cashbox_col == user.cashbox_id,
        )
    )

    return schemas.FolderEntityView(
        entity_id=item.entity_id,
        entity_type=item.entity_type,
        entity_data=dict(entity),
    )


@router.delete("/folders/{folder_id}/items/")
async def delete_folder_item(token: str, idx: int, item: schemas.FolderEntity):
    user = await get_user_by_token(token)

    if not (await folder_exists(idx, user.cashbox_id)):
        raise HTTPException(status_code=404, detail="Такой папки нет!")

    if not (await item_exists_in_folder(idx, item)):
        raise HTTPException(status_code=409, detail="Этого объекта нету в папке!")

    update_query = (
        update(folders_entities)
        .where(
            folders_entities.c.folder_id == idx,
            folders_entities.c.entity_type == item.entity_type,
            folders_entities.c.entity_id == item.entity_id,
        )
        .values(is_deleted=True)
    )

    await database.execute(update_query)

    return Response(status_code=204)
