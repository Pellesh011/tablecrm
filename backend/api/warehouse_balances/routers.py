from collections import defaultdict
from datetime import datetime
from typing import Optional

from api.pagination.pagination import Page
from database.db import (
    categories,
    database,
    nomenclature,
    organizations,
    warehouse_balances_latest,
    warehouse_register_movement,
    warehouses,
)
from fastapi import APIRouter, HTTPException, status
from fastapi_pagination import add_pagination, paginate
from functions.helpers import (
    check_entity_exists,
    datetime_to_timestamp,
    get_user_by_token,
)
from functions.warehouse_events import (
    build_balance_recalc_events,
    publish_balance_recalc_batch,
)
from sqlalchemy import case, func, select

from . import schemas

router = APIRouter(tags=["warehouse_balances"])


def _normalize_amount(value: float) -> float:
    """Нормализуем флоат, чтобы не было огромных остатков"""

    AMOUNT_SCALE = 3
    AMOUNT_EPS = 1e-9

    rounded = round(float(value), AMOUNT_SCALE)
    if abs(rounded) < AMOUNT_EPS:
        return 0.0
    return rounded


@router.get(
    "/warehouse_balances/clearQuantity/category/{id_category}",
    status_code=status.HTTP_202_ACCEPTED,
)
async def clear_quantity(
    token: str,
    id_category: int,
    warehouse_id: int,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
):
    try:
        await get_user_by_token(token)

        dates_arr = []

        if date_to and not date_from:
            dates_arr.append(
                warehouse_register_movement.c.created_at
                <= datetime.fromtimestamp(date_to)
            )
        if date_to and date_from:
            dates_arr.append(
                warehouse_register_movement.c.created_at
                <= datetime.fromtimestamp(date_to)
            )
            dates_arr.append(
                warehouse_register_movement.c.created_at
                >= datetime.fromtimestamp(date_from)
            )
        if not date_to and date_from:
            dates_arr.append(
                warehouse_register_movement.c.created_at
                >= datetime.fromtimestamp(date_from)
            )

        selection_conditions = [
            warehouse_register_movement.c.warehouse_id == warehouse_id,
            nomenclature.c.category == id_category,
            *dates_arr,
        ]

        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.category,
                warehouse_register_movement.c.id.label("id_movement"),
                warehouse_register_movement.c.type_amount,
                warehouse_register_movement.c.amount,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id,
                warehouse_register_movement.c.nomenclature_id,
                warehouse_register_movement.c.cashbox_id,
            ).where(*selection_conditions)
        ).select_from(
            warehouse_register_movement.join(
                nomenclature,
                warehouse_register_movement.c.nomenclature_id == nomenclature.c.id,
            )
        )

        warehouse_balances_db = await database.fetch_all(query)
        recalc_events = build_balance_recalc_events(warehouse_balances_db)
        query_delete_warehouse_register_movement_by_category = (
            warehouse_register_movement.delete().where(
                warehouse_register_movement.c.id.in_(
                    [item.id_movement for item in warehouse_balances_db]
                )
            )
        )
        await database.execute(query_delete_warehouse_register_movement_by_category)
        if recalc_events:
            await publish_balance_recalc_batch(recalc_events)
        return warehouse_balances_db
    except HTTPException as e:
        raise HTTPException(status_code=432, detail=str(e.detail))


@router.get(
    "/warehouse_balances/clearQuantity/product/{id_product}",
    status_code=status.HTTP_202_ACCEPTED,
)
async def clear_quantity(
    token: str,
    id_product: int,
    warehouse_id: int,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
):
    try:
        await get_user_by_token(token)

        dates_arr = []

        if date_to and not date_from:
            dates_arr.append(
                warehouse_register_movement.c.created_at
                <= datetime.fromtimestamp(date_to)
            )
        if date_to and date_from:
            dates_arr.append(
                warehouse_register_movement.c.created_at
                <= datetime.fromtimestamp(date_to)
            )
            dates_arr.append(
                warehouse_register_movement.c.created_at
                >= datetime.fromtimestamp(date_from)
            )
        if not date_to and date_from:
            dates_arr.append(
                warehouse_register_movement.c.created_at
                >= datetime.fromtimestamp(date_from)
            )

        selection_conditions = [
            warehouse_register_movement.c.warehouse_id == warehouse_id,
            nomenclature.c.id == id_product,
            *dates_arr,
        ]

        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.category,
                warehouse_register_movement.c.id.label("id_movement"),
                warehouse_register_movement.c.type_amount,
                warehouse_register_movement.c.amount,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id,
                warehouse_register_movement.c.nomenclature_id,
                warehouse_register_movement.c.cashbox_id,
            ).where(*selection_conditions)
        ).select_from(
            warehouse_register_movement.join(
                nomenclature,
                warehouse_register_movement.c.nomenclature_id == nomenclature.c.id,
            )
        )

        warehouse_balances_db = await database.fetch_one(query)

        if warehouse_balances_db:
            recalc_events = build_balance_recalc_events([warehouse_balances_db])
            query_delete_warehouse_register_movement_by_category = (
                warehouse_register_movement.delete().where(
                    warehouse_register_movement.c.id
                    == warehouse_balances_db.get("id_movement")
                )
            )
            await database.execute(query_delete_warehouse_register_movement_by_category)
            if recalc_events:
                await publish_balance_recalc_batch(recalc_events)
            return warehouse_balances_db
    except HTTPException as e:
        raise HTTPException(status_code=432, detail=str(e.detail))


@router.get("/warehouse_balances/{warehouse_id}/", response_model=int)
async def get_warehouse_current_balance(
    token: str, warehouse_id: int, nomenclature_id: int, organization_id: int
):
    """Получение текущего остатка товара по складу"""
    await get_user_by_token(token)
    await check_entity_exists(warehouses, warehouse_id)
    query = warehouse_balances_latest.select().where(
        warehouse_balances_latest.c.warehouse_id == warehouse_id,
        warehouse_balances_latest.c.nomenclature_id == nomenclature_id,
        warehouse_balances_latest.c.organization_id == organization_id,
    )
    warehouse_db = await database.fetch_one(query)
    if not warehouse_db:
        return 0
    return warehouse_db.current_amount


@router.get("/warehouse_balances/", response_model=Page[schemas.View])
async def get_warehouse_balances(
    token: str,
    warehouse_id: int,
    nomenclature_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Получение списка остатков склада"""
    await get_user_by_token(token)
    query = (
        select(warehouse_balances_latest)
        .where(warehouse_balances_latest.c.warehouse_id == warehouse_id)
        .limit(limit)
        .offset(offset)
    )
    if nomenclature_id is not None:
        query = query.where(
            warehouse_balances_latest.c.nomenclature_id == nomenclature_id
        )
    if organization_id is not None:
        query = query.where(
            warehouse_balances_latest.c.organization_id == organization_id
        )
    warehouse_balances_db = await database.fetch_all(query)
    warehouse_balances_db = [
        {
            **dict(item),
            "created_at": item["updated_at"],
        }
        for item in warehouse_balances_db
    ]
    warehouse_balances_db = [*map(datetime_to_timestamp, warehouse_balances_db)]
    return paginate(warehouse_balances_db)


@router.get("/alt_warehouse_balances/", response_model=schemas.ViewRes)
async def alt_get_warehouse_balances(
    token: str,
    warehouse_id: int,
    nomenclature_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Получение списка остатков склада"""

    await get_user_by_token(token)

    dates_arr = []

    if date_to and not date_from:
        dates_arr.append(
            warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to)
        )
    if date_to and date_from:
        dates_arr.append(
            warehouse_register_movement.c.created_at <= datetime.fromtimestamp(date_to)
        )
        dates_arr.append(
            warehouse_register_movement.c.created_at
            >= datetime.fromtimestamp(date_from)
        )
    if not date_to and date_from:
        dates_arr.append(
            warehouse_register_movement.c.created_at
            >= datetime.fromtimestamp(date_from)
        )

    selection_conditions = [
        warehouse_register_movement.c.warehouse_id == warehouse_id,
        *dates_arr,
    ]
    if nomenclature_id is not None:
        selection_conditions.append(
            warehouse_register_movement.c.nomenclature_id == nomenclature_id
        )
    if organization_id is not None:
        selection_conditions.append(
            warehouse_register_movement.c.organization_id == organization_id
        )

    if dates_arr:
        q = case(
            [
                (
                    warehouse_register_movement.c.type_amount == "minus",
                    warehouse_register_movement.c.amount * (-1),
                )
            ],
            else_=warehouse_register_movement.c.amount,
        )
        query = (
            (
                select(
                    nomenclature.c.id,
                    nomenclature.c.name,
                    nomenclature.c.category,
                    warehouse_register_movement.c.organization_id,
                    warehouse_register_movement.c.warehouse_id,
                    func.sum(q).label("current_amount"),
                ).where(*selection_conditions)
            )
            .group_by(
                nomenclature.c.name,
                nomenclature.c.id,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id,
            )
            .order_by(
                nomenclature.c.category,
                nomenclature.c.name,
                warehouse_register_movement.c.organization_id,
            )
            .select_from(
                warehouse_register_movement.join(
                    nomenclature,
                    warehouse_register_movement.c.nomenclature_id == nomenclature.c.id,
                )
            )
        )
    else:
        latest_conditions = [warehouse_balances_latest.c.warehouse_id == warehouse_id]
        if nomenclature_id is not None:
            latest_conditions.append(
                warehouse_balances_latest.c.nomenclature_id == nomenclature_id
            )
        if organization_id is not None:
            latest_conditions.append(
                warehouse_balances_latest.c.organization_id == organization_id
            )
        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.category,
                warehouse_balances_latest.c.organization_id,
                warehouse_balances_latest.c.warehouse_id,
                warehouse_balances_latest.c.current_amount,
            )
            .where(*latest_conditions)
            .select_from(
                warehouse_balances_latest.join(
                    nomenclature,
                    warehouse_balances_latest.c.nomenclature_id == nomenclature.c.id,
                )
            )
            .order_by(
                nomenclature.c.category,
                nomenclature.c.name,
                warehouse_balances_latest.c.organization_id,
            )
        )

    warehouse_balances_db = await database.fetch_all(query)

    if not warehouse_balances_db:
        return {"result": [{"name": "Без категории", "key": 0, "children": []}]}

    organization_ids = {
        item.organization_id
        for item in warehouse_balances_db
        if item.organization_id is not None
    }
    nomenclature_ids = {item.id for item in warehouse_balances_db}

    current_conditions = [
        warehouse_balances_latest.c.warehouse_id == warehouse_id,
        warehouse_balances_latest.c.nomenclature_id.in_(nomenclature_ids),
    ]
    if organization_ids:
        current_conditions.append(
            warehouse_balances_latest.c.organization_id.in_(organization_ids)
        )
    if organization_id is not None:
        current_conditions.append(
            warehouse_balances_latest.c.organization_id == organization_id
        )

    current_query = select(
        warehouse_balances_latest.c.organization_id,
        warehouse_balances_latest.c.warehouse_id,
        warehouse_balances_latest.c.nomenclature_id.label("id"),
        warehouse_balances_latest.c.current_amount,
    ).where(*current_conditions)

    warehouse_balances_db_curr = await database.fetch_all(current_query)
    current_map = {
        (item.organization_id, item.warehouse_id, item.id): item.current_amount
        for item in warehouse_balances_db_curr
    }

    organization_map = {}
    if organization_ids:
        organizations_db = await database.fetch_all(
            select(organizations.c.id, organizations.c.short_name).where(
                organizations.c.id.in_(organization_ids)
            )
        )
        organization_map = {
            item.id: item.short_name for item in organizations_db if item.id is not None
        }

    warehouse_name = None
    warehouse_db = await database.fetch_one(
        select(warehouses.c.id, warehouses.c.name).where(
            warehouses.c.id == warehouse_id
        )
    )
    if warehouse_db:
        warehouse_name = warehouse_db.name

    plus_minus_query = (
        select(
            warehouse_register_movement.c.organization_id,
            warehouse_register_movement.c.warehouse_id,
            warehouse_register_movement.c.nomenclature_id.label("id"),
            func.coalesce(
                func.sum(
                    case(
                        [
                            (
                                warehouse_register_movement.c.type_amount == "plus",
                                warehouse_register_movement.c.amount,
                            )
                        ],
                        else_=0,
                    )
                ),
                0,
            ).label("plus_amount"),
            func.coalesce(
                func.sum(
                    case(
                        [
                            (
                                warehouse_register_movement.c.type_amount == "minus",
                                warehouse_register_movement.c.amount,
                            )
                        ],
                        else_=0,
                    )
                ),
                0,
            ).label("minus_amount"),
        )
        .where(
            warehouse_register_movement.c.warehouse_id == warehouse_id,
            warehouse_register_movement.c.nomenclature_id.in_(nomenclature_ids),
            *dates_arr,
        )
        .group_by(
            warehouse_register_movement.c.organization_id,
            warehouse_register_movement.c.warehouse_id,
            warehouse_register_movement.c.nomenclature_id,
        )
    )
    if organization_ids:
        plus_minus_query = plus_minus_query.where(
            warehouse_register_movement.c.organization_id.in_(organization_ids)
        )
    if organization_id is not None:
        plus_minus_query = plus_minus_query.where(
            warehouse_register_movement.c.organization_id == organization_id
        )

    plus_minus_rows = await database.fetch_all(plus_minus_query)
    plus_minus_map = {
        (item.organization_id, item.warehouse_id, item.id): (
            item.plus_amount,
            item.minus_amount,
        )
        for item in plus_minus_rows
    }

    categories_db = await database.fetch_all(categories.select())
    grouped_children = defaultdict(list)
    uncategorized = []

    for warehouse_balance in warehouse_balances_db:
        key = (
            warehouse_balance.organization_id,
            warehouse_balance.warehouse_id,
            warehouse_balance.id,
        )
        plus_amount, minus_amount = plus_minus_map.get(key, (0, 0))

        balance_dict = dict(warehouse_balance)
        balance_dict["current_amount"] = _normalize_amount(
            balance_dict["current_amount"]
        )
        balance_dict["plus_amount"] = _normalize_amount(plus_amount)
        balance_dict["minus_amount"] = _normalize_amount(minus_amount)
        balance_dict["start_ost"] = _normalize_amount(
            balance_dict["current_amount"] - plus_amount + minus_amount
        )
        balance_dict["now_ost"] = _normalize_amount(
            current_map.get(key, balance_dict["current_amount"])
        )
        balance_dict["organization_name"] = organization_map.get(
            warehouse_balance.organization_id
        )
        balance_dict["warehouse_name"] = warehouse_name

        if balance_dict["category"] is None:
            uncategorized.append(balance_dict)
        else:
            grouped_children[balance_dict["category"]].append(balance_dict)

    res_with_cats = []
    for category in categories_db:
        cat_childrens = grouped_children.get(category.id, [])
        if cat_childrens:
            res_with_cats.append(
                {"name": category.name, "key": category.id, "children": cat_childrens}
            )

    res_with_cats.append({"name": "Без категории", "key": 0, "children": uncategorized})

    return {"result": res_with_cats}


add_pagination(router)
