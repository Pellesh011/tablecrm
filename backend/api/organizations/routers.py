import api.organizations.schemas as schemas
from database.db import database, organizations
from fastapi import APIRouter
from functions.helpers import datetime_to_timestamp, get_entity_by_id, get_user_by_token
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["organizations"])


@router.get("/organizations/{idx}/", response_model=schemas.Organization)
async def get_organization_by_id(token: str, idx: int):
    """Получение организации по ID"""
    user = await get_user_by_token(token)
    organization_db = await get_entity_by_id(organizations, idx, user.cashbox_id)
    organization_db = datetime_to_timestamp(organization_db)
    return organization_db


@router.get("/organizations/", response_model=schemas.OrganizationListGet)
async def get_organizations(token: str, limit: int = 100, offset: int = 0):
    """Получение списка компаний"""
    user = await get_user_by_token(token)

    query = (
        organizations.select()
        .where(
            organizations.c.cashbox == user.cashbox_id,
            organizations.c.is_deleted.is_not(True),
        )
        .limit(limit)
        .offset(offset)
    )

    organizations_db = await database.fetch_all(query)

    query = select(func.count(organizations.c.id)).where(
        organizations.c.cashbox == user.cashbox_id,
        organizations.c.is_deleted.is_not(True),
    )

    organizations_db_count = await database.fetch_one(query)

    organizations_db = [*map(datetime_to_timestamp, organizations_db)]

    return {"result": organizations_db, "count": organizations_db_count.count_1}


@router.post("/organizations/", response_model=schemas.Organization)
async def new_organization(token: str, organization: schemas.OrganizationCreate):
    """Создание организации"""
    user = await get_user_by_token(token)

    organization_values = organization.dict()
    organization_values["owner"] = user.id
    organization_values["cashbox"] = user.cashbox_id
    organization_values["is_deleted"] = False

    query = organizations.insert().values(organization_values)
    organization_id = await database.execute(query)

    query = organizations.select().where(
        organizations.c.id == organization_id,
        organizations.c.cashbox == user.cashbox_id,
    )
    organization_db = await database.fetch_one(query)
    organization_db = datetime_to_timestamp(organization_db)

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "organizations",
            "result": organization_db,
        },
    )

    return organization_db


@router.patch("/organizations/{idx}/", response_model=schemas.Organization)
async def edit_organization(
    token: str,
    idx: int,
    organization: schemas.OrganizationEdit,
):
    """Редактирование организации"""
    user = await get_user_by_token(token)
    organization_db = await get_entity_by_id(organizations, idx, user.cashbox_id)
    organization_values = organization.dict(exclude_unset=True)

    if organization_values:
        query = (
            organizations.update()
            .where(
                organizations.c.id == idx, organizations.c.cashbox == user.cashbox_id
            )
            .values(organization_values)
        )
        await database.execute(query)
        organization_db = await get_entity_by_id(organizations, idx, user.cashbox_id)

    organization_db = datetime_to_timestamp(organization_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "organizations", "result": organization_db},
    )

    return organization_db


@router.delete("/organizations/{idx}/", response_model=schemas.Organization)
async def delete_organization(token: str, idx: int):
    """Удаление организации"""
    user = await get_user_by_token(token)

    await get_entity_by_id(organizations, idx, user.cashbox_id)

    query = (
        organizations.update()
        .where(organizations.c.id == idx, organizations.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = organizations.select().where(
        organizations.c.id == idx, organizations.c.cashbox == user.cashbox_id
    )
    organization_db = await database.fetch_one(query)
    organization_db = datetime_to_timestamp(organization_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "organizations",
            "result": organization_db,
        },
    )

    return organization_db
