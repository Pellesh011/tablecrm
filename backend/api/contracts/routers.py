from typing import Optional

import api.contracts.schemas as schemas
from database.db import contracts, database, organizations
from fastapi import APIRouter, HTTPException
from functions.helpers import (
    check_contragent_exists,
    check_entity_exists,
    datetime_to_timestamp,
    get_entity_by_id,
    get_user_by_token,
)
from sqlalchemy import func, select
from ws_manager import manager

router = APIRouter(tags=["contracts"])


@router.get("/contracts/{idx}/", response_model=schemas.Contract)
async def get_contract_by_id(token: str, idx: int):
    """Получение контракта по ID"""
    user = await get_user_by_token(token)
    contract_db = await get_entity_by_id(contracts, idx, user.id)
    contract_db = datetime_to_timestamp(contract_db)
    return contract_db


@router.get("/contracts/", response_model=schemas.ContractListGet)
async def get_contracts(
    token: str, name: Optional[str] = None, limit: int = 100, offset: int = 0
):
    """Получение списка контрактов"""
    user = await get_user_by_token(token)
    filters = [
        contracts.c.cashbox == user.cashbox_id,
        contracts.c.is_deleted.is_not(True),
    ]

    if name:
        filters.append(contracts.c.name.ilike(f"%{name}%"))

    query = contracts.select().where(*filters).limit(limit).offset(offset)

    contracts_db = await database.fetch_all(query)
    contracts_db = [*map(datetime_to_timestamp, contracts_db)]

    query = select(func.count(contracts.c.id)).where(*filters)

    contracts_db_count = await database.fetch_one(query)

    return {"result": contracts_db, "count": contracts_db_count.count_1}


@router.post("/contracts/", response_model=schemas.ContractList)
async def new_contract(token: str, contracts_data: schemas.ContractCreateMass):
    """Создание контракта"""
    user = await get_user_by_token(token)

    inserted_ids = set()
    contragents_cache = set()
    organizations_cache = set()
    exceptions = []
    for contract_values in contracts_data.dict()["__root__"]:
        contract_values["owner"] = user.id
        contract_values["cashbox"] = user.cashbox_id

        if contract_values.get("contragent") is not None:
            if contract_values["contragent"] not in contragents_cache:
                try:
                    await check_contragent_exists(
                        contract_values["contragent"], user.cashbox_id
                    )
                    contragents_cache.add(contract_values["contragent"])
                except HTTPException as e:
                    exceptions.append(str(contract_values) + " " + e.detail)
                    continue

        if contract_values.get("organization") is not None:
            if contract_values["organization"] not in organizations_cache:
                try:
                    await check_entity_exists(
                        organizations, contract_values["organization"], user.id
                    )
                    organizations_cache.add(contract_values["organization"])
                except HTTPException as e:
                    exceptions.append(str(contract_values) + " " + e.detail)
                    continue

        query = contracts.insert().values(contract_values)
        contract_id = await database.execute(query)
        inserted_ids.add(contract_id)

    query = contracts.select().where(
        contracts.c.cashbox == user.cashbox_id, contracts.c.id.in_(inserted_ids)
    )
    contracts_db = await database.fetch_all(query)
    contracts_db = [*map(datetime_to_timestamp, contracts_db)]

    await manager.send_message(
        token,
        {
            "action": "create",
            "target": "contracts",
            "result": contracts_db,
        },
    )

    if exceptions:
        raise HTTPException(
            400, "Не были добавлены следующие записи: " + ", ".join(exceptions)
        )

    return contracts_db


@router.patch("/contracts/{idx}/", response_model=schemas.Contract)
async def edit_contract(
    token: str,
    idx: int,
    contract: schemas.ContractEdit,
):
    """Редактирование контракта"""
    user = await get_user_by_token(token)
    contract_db = await get_entity_by_id(contracts, idx, user.id)
    contract_values = contract.dict(exclude_unset=True)

    if contract_values:
        if contract_values.get("contragent") is not None:
            await check_contragent_exists(
                contract_values["contragent"], user.cashbox_id
            )

        if contract_values.get("organization") is not None:
            await check_entity_exists(
                organizations, contract_values["organization"], user.id
            )

        query = (
            contracts.update()
            .where(contracts.c.id == idx, contracts.c.cashbox == user.cashbox_id)
            .values(contract_values)
        )
        await database.execute(query)
        contract_db = await get_entity_by_id(contracts, idx, user.id)

    contract_db = datetime_to_timestamp(contract_db)

    await manager.send_message(
        token,
        {"action": "edit", "target": "contracts", "result": contract_db},
    )

    return contract_db


@router.delete("/contracts/{idx}/", response_model=schemas.Contract)
async def delete_contract(token: str, idx: int):
    """Удаление контракта"""
    user = await get_user_by_token(token)

    await get_entity_by_id(contracts, idx, user.id)

    query = (
        contracts.update()
        .where(contracts.c.id == idx, contracts.c.cashbox == user.cashbox_id)
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = contracts.select().where(
        contracts.c.id == idx, contracts.c.cashbox == user.cashbox_id
    )
    contract_db = await database.fetch_one(query)
    contract_db = datetime_to_timestamp(contract_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "contracts",
            "result": contract_db,
        },
    )

    return contract_db
