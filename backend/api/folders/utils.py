from api.folders import schemas
from database import db
from database.db import database, folders, folders_entities
from sqlalchemy import exists, select


def get_table_by_name(table_name: str):
    table_obj = getattr(db, table_name)

    return table_obj


def get_column_by_names(table_obj, possible_names: list[str]):
    for name in possible_names:
        if hasattr(table_obj.c, name):
            return getattr(table_obj.c, name)
    return None


async def folder_exists(folder_id: int, cashbox_id: int):
    folder_exists_query = exists().where(
        folders.c.id == folder_id,
        folders.c.cashbox_id == cashbox_id,
        folders.c.is_deleted == False,
        folders.c.is_active == True,
    )

    return (
        await database.fetch_val(select([1]).where(folder_exists_query))
    ) is not None


async def get_folders(cashbox_id: int, predicates: list = None):
    if predicates is None:
        predicates = []

    predicates.append(folders.c.is_deleted == False)
    predicates.append(folders.c.cashbox_id == cashbox_id)

    folders_query = select(folders).where(*predicates)

    return await database.fetch_all(folders_query)


async def item_exists(cashbox_id: int, item: schemas.FolderEntity):
    item_dict = item.dict(exclude_none=True)
    entity_table = get_table_by_name(item_dict.get("entity_type"))
    cashbox_col = get_column_by_names(entity_table, ["cashbox", "cashbox_id"])

    item_exists_query = exists().where(
        cashbox_col == cashbox_id,
        entity_table.c.id == item_dict.get("entity_id"),
        entity_table.c.is_deleted == False,
    )

    return (await database.fetch_val(select([1]).where(item_exists_query))) is not None


async def item_exists_in_folder(folder_id: int, item: schemas.FolderEntity):
    item_dict = item.dict(exclude_none=True)

    item_exists_query = exists().where(
        folders_entities.c.folder_id == folder_id,
        folders_entities.c.entity_id == item_dict.get("entity_id"),
        folders_entities.c.entity_type == item_dict.get("entity_type"),
    )

    return (await database.fetch_val(select([1]).where(item_exists_query))) is not None
