from datetime import datetime
from typing import Optional

import api.webapp.schemas as schemas
from database.db import (
    categories,
    database,
    manufacturers,
    nomenclature,
    organizations,
    pictures,
    price_types,
    prices,
    units,
    warehouse_balances_latest,
    warehouse_register_movement,
    warehouses,
)
from fastapi import APIRouter, Query
from functions.helpers import (
    datetime_to_timestamp,
    get_user_by_token,
    nomenclature_unit_id_to_name,
    raise_bad_request,
)
from sqlalchemy import case, func, select

router = APIRouter(tags=["webapp"])


# TODO: refactor warehouses part, split into separate functions for readability, do queries before loop (perf)
@router.get("/webapp/", response_model=schemas.WebappResponse)
async def get_nomenclature(
    token: str = Query(
        ...,
        description="Токен для аутентификации. "
        "Запрос вернёт только те объекты, которые принадлежат пользователю.",
    ),
    warehouse_id: Optional[int] = None,
    nomenclature_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    name: Optional[str] = Query(None, description="Полное название или его часть."),
    type: Optional[str] = Query(
        None, description="Тип полностью (напр., product или service)."
    ),
    description_short: Optional[str] = Query(
        None, description="Короткое описание или его часть."
    ),
    description_long: Optional[str] = Query(
        None, description="Подробное описание или его часть."
    ),
    code: Optional[int] = None,
    unit: Optional[int] = None,
    category_ids: Optional[str] = Query(
        None,
        description="Целочисленные идентификаторы категорий. "
        "Несколько категорий указываются через запятую.",
    ),
    manufacturer: Optional[str] = Query(
        None, description="Название производителя или его часть."
    ),
    date_from: Optional[int] = Query(
        None, description="Начальная дата для фильтрации по дате создания."
    ),
    date_to: Optional[int] = Query(
        None, description="Конечная дата для фильтрации по дате создания."
    ),
    entity: Optional[str] = Query(
        None, description="Entity для фильтрации по изображениям."
    ),
    entity_id: Optional[int] = Query(
        None, description="Entity ID для фильтрации по изображениям."
    ),
    price_type_id: Optional[int] = Query(
        None, description="Price Type ID для фильтрации по ценам."
    ),
    limit: int = 100,
    offset: int = 0,
):
    """Получение номенклатуры по поисковым параметрам. Содержит название,
    тип (Товар или Услуга), описание, код, единицы, названия категорий,
    производителя, дату-время создания и обновления, фотографии, цены и
    складские балансы. Требует действительный токен владельца кассы / товаров"""

    user = await get_user_by_token(token)

    filter_general = [
        nomenclature.c.cashbox == user.cashbox_id,
        nomenclature.c.is_deleted.is_not(True),
    ]
    filter_pics = []
    filter_prices = []
    filter_balance = []

    if name:
        filter_general.append(nomenclature.c.name.ilike(f"%{name}%"))
    if type:
        filter_general.append(nomenclature.c.type.ilike(f"{type}"))
    if description_short:
        filter_general.append(
            nomenclature.c.description_short.ilike(f"%{description_short}%")
        )
    if description_long:
        filter_general.append(
            nomenclature.c.description_long.ilike(f"%{description_long}%")
        )
    if code:
        filter_general.append(nomenclature.c.code == code)
    if unit:
        filter_general.append(nomenclature.c.unit == unit)
    if category_ids:
        try:
            category_ids = [int(ix) for ix in category_ids.split(",")]
        except ValueError:
            raise_bad_request("Category IDs must contain only numbers")
        filter_general.append(nomenclature.c.category.in_(category_ids))
    if nomenclature_id:
        filter_general.append(nomenclature.c.id == nomenclature_id)
    if warehouse_id:
        filter_balance.append(warehouse_balances_latest.c.warehouse_id == warehouse_id)
    if organization_id:
        filter_balance.append(
            warehouse_balances_latest.c.organization_id == organization_id
        )
    if filter_balance:
        filter_balance.append(warehouse_balances_latest.c.current_amount > 0)
    if manufacturer:
        filter_general.append(nomenclature.c.manufacturer.ilike(f"%{manufacturer}%"))

    if entity or entity_id:
        filter_pics.append(pictures.c.is_deleted.is_not(True))
        filter_pics.append(pictures.c.owner == user.id)
        if entity:
            filter_pics.append(pictures.c.entity == entity)
        if entity_id:
            filter_pics.append(pictures.c.entity_id == entity_id)

    if price_type_id:
        filter_prices.append(prices.c.is_deleted.is_not(True))
        filter_prices.append(prices.c.owner == user.id)
        if price_type_id:
            filter_prices.append(prices.c.price_type == price_type_id)

    query = (
        select(nomenclature)
        .distinct(nomenclature.c.id)
        .select_from(
            nomenclature.join(
                prices, nomenclature.c.id == prices.c.nomenclature, isouter=True
            )
            .join(pictures, nomenclature.c.id == pictures.c.entity_id, isouter=True)
            .join(
                warehouse_balances_latest,
                nomenclature.c.id == warehouse_balances_latest.c.nomenclature_id,
                isouter=True,
            )
        )
        .where(*filter_general, *filter_pics, *filter_prices, *filter_balance)
        .limit(limit)
        .offset(offset)
    )

    result_nomenclature = await database.fetch_all(query)
    result_nomenclature = [*map(datetime_to_timestamp, result_nomenclature)]

    result_nomenclature = [*map(nomenclature_unit_id_to_name, result_nomenclature)]
    result_nomenclature = [await d for d in result_nomenclature]

    count_query = select(func.count()).select_from(
        select(nomenclature.c.id)
        .distinct(nomenclature.c.id)
        .select_from(
            nomenclature.join(
                prices, nomenclature.c.id == prices.c.nomenclature, isouter=True
            )
            .join(pictures, nomenclature.c.id == pictures.c.entity_id, isouter=True)
            .join(
                warehouse_balances_latest,
                nomenclature.c.id == warehouse_balances_latest.c.nomenclature_id,
                isouter=True,
            )
        )
        .where(*filter_general, *filter_pics, *filter_prices, *filter_balance)
        .alias()
    )

    nomenclature_db_c = await database.fetch_one(count_query)

    # post-process:
    for item in result_nomenclature:
        # getting pictures list ([pictures/schemas.py/Picture])
        query = pictures.select().where(
            pictures.c.entity_id == item["id"], pictures.c.is_deleted.is_not(True)
        )
        pictures_list = await database.fetch_all(query)
        pictures_list = [*map(datetime_to_timestamp, pictures_list)]
        item["pictures"] = pictures_list

        # getting prices list ([webapp/schemas.py/PriceInList])
        query = prices.select().where(
            prices.c.nomenclature == item["id"],
            prices.c.is_deleted.is_not(True),
            prices.c.owner == user.id,
        )
        prices_list = await database.fetch_all(query)

        item["prices"] = []
        for price_item in prices_list:
            price_in_list = {
                "id": price_item.id,
                "unit_name": None,
                "category_name": None,
                "manufacturer_name": None,
                "price": price_item.price,
                "date_from": price_item.date_from,
                "date_to": price_item.date_to,
                "price_types": [],
            }
            if item["unit"]:
                q = units.select().where(units.c.id == item["unit"])
                unit = await database.fetch_one(q)
                if unit:
                    price_in_list["unit_name"] = unit.name

            if item["category"]:
                q = categories.select().where(categories.c.id == item["category"])
                category = await database.fetch_one(q)
                if category:
                    price_in_list["category_name"] = category.name

            if item["manufacturer"]:
                q = manufacturers.select().where(
                    manufacturers.c.id == item["manufacturer"]
                )
                manufacturer = await database.fetch_one(q)
                if manufacturer:
                    price_in_list["manufacturer_name"] = manufacturer.name

            if price_item["price_type"]:
                q = price_types.select().where(
                    price_types.c.id == price_item["price_type"]
                )
                price_type = await database.fetch_one(q)

                if price_type:
                    q = price_types.select().where(
                        price_types.c.name == price_type.name,
                        price_types.c.owner == user.id,
                        price_types.c.is_deleted.is_not(True),
                    )
                    price_types_list = await database.fetch_all(q)
                    price_types_list = [*map(datetime_to_timestamp, price_types_list)]
                    price_in_list["price_types"] = price_types_list.copy()

            item["prices"].append(price_in_list.copy())

        # getting warehouse balances list ([webapp/schemas.py/ViewAltList])
        # (alsk: just a small refactor of previous code, no real changes)
        sel_condition = [warehouse_register_movement.c.nomenclature_id == item["id"]]

        if warehouse_id:
            sel_condition.append(
                warehouse_register_movement.c.warehouse_id == warehouse_id
            )
        if nomenclature_id:
            sel_condition.append(
                warehouse_register_movement.c.nomenclature_id == nomenclature_id
            )
        if organization_id:
            sel_condition.append(
                warehouse_register_movement.c.organization_id == organization_id
            )

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
                )
                .where(*sel_condition)
                .limit(limit)
                .offset(offset)
            )
            .group_by(
                nomenclature.c.name,
                nomenclature.c.id,
                warehouse_register_movement.c.organization_id,
                warehouse_register_movement.c.warehouse_id,
            )
            .select_from(
                warehouse_register_movement.join(
                    nomenclature,
                    warehouse_register_movement.c.nomenclature_id == nomenclature.c.id,
                )
            )
        )

        warehouse_balances_db = await database.fetch_all(query)

        date_condition = []

        # no need to query twice if nor date_to neither date_from is present, just copying previous DB response
        if date_to or date_from:
            if date_to:
                date_condition.append(
                    warehouse_register_movement.c.created_at
                    <= datetime.fromtimestamp(date_to)
                )
            if date_from:
                date_condition.append(
                    warehouse_register_movement.c.created_at
                    >= datetime.fromtimestamp(date_from)
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
                    )
                    .where(*sel_condition, *date_condition)
                    .limit(limit)
                    .offset(offset)
                )
                .group_by(
                    nomenclature.c.name,
                    nomenclature.c.id,
                    warehouse_register_movement.c.organization_id,
                    warehouse_register_movement.c.warehouse_id,
                )
                .select_from(
                    warehouse_register_movement.join(
                        nomenclature,
                        warehouse_register_movement.c.nomenclature_id
                        == nomenclature.c.id,
                    )
                )
            )

            warehouse_balances_db_curr = await database.fetch_all(query)
        else:
            warehouse_balances_db_curr = warehouse_balances_db.copy()
        # # #

        res = []

        categories_db = await database.fetch_all(categories.select())

        res_with_cats = []

        for warehouse_balance in warehouse_balances_db:

            current = [
                item
                for item in warehouse_balances_db_curr
                if item.id == warehouse_balance.id
            ]

            balance_dict = dict(warehouse_balance)

            organization_db = await database.fetch_one(
                organizations.select().where(
                    organizations.c.id == warehouse_balance.organization_id
                )
            )

            plus_amount = 0
            minus_amount = 0

            register_q = (
                warehouse_register_movement.select()
                .where(
                    warehouse_register_movement.c.warehouse_id == warehouse_id,
                    warehouse_register_movement.c.nomenclature_id
                    == warehouse_balance.id,
                    *date_condition,
                )
                .order_by(warehouse_register_movement.c.id)
            )

            register_events = await database.fetch_all(register_q)

            for reg_event in register_events:
                if reg_event.type_amount == "plus":
                    plus_amount += reg_event.amount
                else:
                    minus_amount += reg_event.amount

            balance_dict["organization_name"] = organization_db.short_name
            balance_dict["plus_amount"] = plus_amount
            balance_dict["minus_amount"] = minus_amount
            balance_dict["start_ost"] = (
                balance_dict["current_amount"] - plus_amount + minus_amount
            )
            balance_dict["now_ost"] = current[0].current_amount
            filter_warehouses = [
                warehouses.c.cashbox == user.cashbox_id,
                warehouses.c.is_deleted.is_not(True),
            ]

            query = warehouses.select().where(
                warehouses.c.id == warehouse_balance.warehouse_id, *filter_warehouses
            )

            warehouses_db = await database.fetch_all(query)
            warehouses_db = [*map(datetime_to_timestamp, warehouses_db)]
            balance_dict["warehouses"] = warehouses_db

            res.append(balance_dict)
        for category in categories_db:
            cat_children = []
            for item_cat in res:
                if item_cat["category"] == category.id:
                    item_cat.pop("id", None)
                    item_cat.pop("name", None)

                    item_cat.pop("warehouse_id", None)
                    cat_children.append(item_cat)

            if len(cat_children) > 0:
                res_with_cats.append(
                    {
                        "name": category.name,
                        "key": category.id,
                        "children": cat_children,
                    }
                )

        if len(res_with_cats) == 0:
            res_with_cats.append({"name": "Без категории", "key": 0, "children": []})
        else:
            for cat in res_with_cats:
                for catinclusive in cat["children"]:
                    catinclusive.pop("category", None)

        item["alt_warehouse_balances"] = res_with_cats

    return {"result": result_nomenclature, "count": nomenclature_db_c.count_1}
