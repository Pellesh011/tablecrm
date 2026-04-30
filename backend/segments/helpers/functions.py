import asyncio
import json

from api.docs_sales.api.routers import generate_and_save_order_links
from database.db import (
    contragents,
    database,
    docs_sales,
    docs_sales_delivery_info,
    docs_sales_goods,
    loyality_cards,
    nomenclature,
    units,
    users,
    users_cboxes_relation,
    warehouses,
)
from sqlalchemy import select


def format_contragent_text_notifications(
    action: str, segment_name: str, name: str, phone: str
):
    if action == "new_contragent":
        header = "Новый пользователь добавлен в сегмент!"
    else:
        header = "Пользователь исключен из сегмента!"
    return f"{header}\nСегмент: {segment_name}.\nКлиент:\n{name}\nТелефон: {phone}"


async def create_replacements(obj_id: int, obj_type: str = "docs_sales") -> dict:
    replacements = {}
    tasks = []
    contragent_id = None
    if obj_type == "docs_sales":
        order_id = obj_id
        tasks += [
            add_warehouse_info_to_replacements(replacements, order_id),
            add_manager_info_to_replacements(replacements, order_id),
            add_docs_sales_goods_info_to_replacements(replacements, order_id),
            link_replacements(replacements, order_id),
            create_delivery_info_text(replacements, order_id),
            order_info_to_replacements(replacements, order_id),
        ]
        query = docs_sales.select().where(docs_sales.c.id == order_id)
        order = await database.fetch_one(query)

        contragent_id = order.contragent

    if obj_type == "contragent":
        contragent_id = obj_id
    if contragent_id:
        tasks += [
            add_contragent_info_to_replacements(replacements, contragent_id),
            add_loyality_card_info_to_replacements(replacements, contragent_id),
        ]

    await asyncio.gather(*tasks)
    return replacements


async def order_info_to_replacements(replacements: dict, order_id: int):
    data = {}
    order_query = docs_sales.select().where(docs_sales.c.id == order_id)
    order = await database.fetch_one(order_query)
    if not order:
        return

    order_statuses = {
        "received": "Получен",
        "processed": "Обработан",
        "collecting": "Собирается",
        "collected": "Собран",
        "picked": "Назначен доставщик",
        "delivered": "Доставлен",
        "closed": "Закрыт",
        "success": "Успешно",
    }
    if order.order_status:
        data["order_status"] = (
            order_statuses[order.order_status]
            if order.order_status in order_statuses
            else order.status
        )

    if order.assigned_picker:
        query = (
            users.select()
            .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
            .where(users_cboxes_relation.c.id == order.assigned_picker)
        )
        user = await database.fetch_one(query)
        if user:
            data["picker_name"] = ""
            if user.first_name:
                data["picker_name"] += f"{user.first_name} "
            if user.last_name:
                data["picker_name"] += f"{user.last_name}"

    if order.assigned_courier:
        query = (
            users.select()
            .join(users_cboxes_relation, users_cboxes_relation.c.user == users.c.id)
            .where(users_cboxes_relation.c.id == order.assigned_courier)
        )
        user = await database.fetch_one(query)
        if user:
            data["courier_name"] = ""
            if user.first_name:
                data["courier_name"] += f"{user.first_name} "
            if user.last_name:
                data["courier_name"] += f"{user.last_name}"

    replacements.update(data)


async def add_warehouse_info_to_replacements(replacements: dict, order_id: int):
    data = {}
    query = (
        warehouses.select()
        .join(docs_sales, docs_sales.c.warehouse == warehouses.c.id)
        .where(docs_sales.c.id == order_id)
    )
    row = await database.fetch_one(query)
    if row:
        if row.name:
            data["warehouse_name"] = row.name
        if row.address:
            data["warehouse_address"] = row.address
        if row.phone:
            data["warehouse_phone"] = row.phone
    replacements.update(data)


async def add_manager_info_to_replacements(replacements: dict, order_id: int):
    data = {}
    order_query = docs_sales.select().where(docs_sales.c.id == order_id)
    order = await database.fetch_one(order_query)
    if not order:
        return
    query = users.select().join(
        users_cboxes_relation, users_cboxes_relation.c.user == users.c.id
    )
    user = await database.fetch_one(query)
    if user:
        data["manager_name"] = ""
        if user.first_name:
            data["manager_name"] += f"{user.first_name} "
        if user.last_name:
            data["manager_name"] += f"{user.last_name}"
        if user.phone_number:
            data["manager_phone"] = user.phone_number

    replacements.update(data)


async def link_replacements(replacements, order_id):
    data = {}
    links = await generate_and_save_order_links(order_id)
    for k, v in links.items():
        data[k] = f"\n\n<a href='{v['url']}'>Открыть заказ</a>"

    replacements.update(data)


async def create_delivery_info_text(replacements: dict, docs_sales_id: int):
    query = docs_sales_delivery_info.select().where(
        docs_sales_delivery_info.c.docs_sales_id == docs_sales_id
    )
    delivery_info = await database.fetch_one(query)
    data = {}
    if delivery_info is None:
        return data
    if delivery_info.address:
        data["delivery_address"] = delivery_info.address
    if delivery_info.note:
        data["delivery_note"] = delivery_info.note
    if delivery_info.delivery_date:
        data["delivery_date"] = delivery_info.delivery_date.strftime("%d.%m.%Y %H:%M")
    if delivery_info.recipient:
        recipient_data = json.loads(delivery_info.recipient)
        if recipient_data:
            data["delivery_recipient_name"] = recipient_data.get("name")
            data["delivery_recipient_phone"] = recipient_data.get("phone")

    replacements.update(data)


async def add_docs_sales_goods_info_to_replacements(
    replacements: dict, docs_sales_id: int
):
    data = {}
    subquery = (
        docs_sales_goods.select()
        .where(docs_sales_goods.c.docs_sales_id == docs_sales_id)
        .subquery("goods")
    )
    query = (
        select(
            nomenclature.c.name,
            subquery.c.price,
            subquery.c.quantity,
            units.c.convent_national_view,
        )
        .outerjoin(nomenclature, subquery.c.nomenclature == nomenclature.c.id)
        .outerjoin(units, subquery.c.unit == units.c.id)
    )
    goods = await database.fetch_all(query)
    if goods:
        sum = 0
        data["goods"] = ""
        for good in goods:
            data["goods"] += (
                f"{good.name} - {good.quantity} "
                f"{good.convent_national_view if good.convent_national_view  else ''}"
                f" x {good.price} р = {good.quantity * good.price} р\n"
            )
            sum += good.quantity * good.price
        data["goods_count"] = len(goods)
        data["order_sum"] = sum
    replacements.update(data)


async def add_contragent_info_to_replacements(replacements: dict, contragent_id: int):
    data = {}

    query = select(contragents).where(contragents.c.id == contragent_id)
    contragent = await database.fetch_one(query)
    if contragent:
        data["contragent_name"] = contragent.name
        data["contragent_phone"] = contragent.phone
        data["contragent_inn"] = contragent.inn

    replacements.update(data)


async def add_loyality_card_info_to_replacements(
    replacements: dict, contragent_id: int
):
    data = {}
    query = (
        select(loyality_cards)
        .join(contragents, contragents.c.id == contragent_id)
        .limit(1)
    )
    card = await database.fetch_one(query)
    if card:
        data["card_number"] = card.card_number
        data["card_balance"] = card.balance
        data["card_income"] = card.income
        data["card_outcome"] = card.outcome
        data["card_cashback_percent"] = card.cashback_percent
        data["card_minimal_checque_amount"] = card.minimal_checque_amount
        data["card_max_percentage"] = card.max_percentage
        data["card_max_withdraw_percentage"] = card.max_withdraw_percentage
    replacements.update(data)
