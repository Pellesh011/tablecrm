import hashlib
import json
import math
import random
import string
from datetime import datetime
from typing import Any, List, Optional, Union

import aiohttp
import pytz
from const import PaymentType
from database.db import (
    articles,
    contragents,
    database,
    docs_sales_delivery_info,
    docs_sales_goods,
    docs_sales_settings,
    entity_or_function,
    entity_to_entity,
    fifo_settings,
    loyality_transactions,
    nomenclature,
    organizations,
    payments,
    units,
    user_permissions,
    users_cboxes_relation,
)
from databases.backends.postgres import Record
from fastapi import HTTPException
from sqlalchemy import String, Table, and_, cast, func, or_
from sqlalchemy.sql import ColumnElement


def gen_token():
    letters = string.ascii_letters
    rand_string = "".join(random.choice(letters) for i in range(32))
    token = hashlib.sha256(rand_string.encode("utf-8")).hexdigest()
    return token


def get_filters(table, filters, cashbox_id: int = None):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "name":
            if value:
                filters_list.append((f"payments.name ILIKE '%{value}%'"))
        elif filter == "tags":
            if value:
                filters_list.append((f"payments.tags ILIKE '%{value}%'"))
        elif filter == "project":
            if value:
                if cashbox_id is not None:
                    filters_list.append(
                        (
                            f"payments.project_id IN (SELECT projects.id FROM projects WHERE projects.name ILIKE '%{value}%' AND projects.cashbox = {int(cashbox_id)})"
                        )
                    )
                else:
                    filters_list.append(
                        (
                            f"payments.project_id IN (SELECT projects.id FROM projects WHERE projects.name ILIKE '%{value}%' AND projects.cashbox = payments.cashbox)"
                        )
                    )
        elif filter == "contragent":
            if value:
                if cashbox_id is not None:
                    filters_list.append(
                        (
                            f"payments.contragent IN (SELECT contragents.id FROM contragents WHERE contragents.name ILIKE '%{value}%' AND contragents.cashbox = {int(cashbox_id)})"
                        )
                    )
                else:
                    filters_list.append(
                        (
                            f"payments.contragent IN (SELECT contragents.id FROM contragents WHERE contragents.name ILIKE '%{value}%' AND contragents.cashbox = payments.cashbox)"
                        )
                    )
        elif filter == "paybox":
            if value:
                is_include_paybox_dest = (
                    str(filters_dict.get("include_paybox_dest", "")).lower() == "true"
                )
                if is_include_paybox_dest:
                    if cashbox_id is not None:
                        filters_list.append(
                            (
                                f"(payments.paybox IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = {int(cashbox_id)}) OR "
                                f"payments.paybox_to IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = {int(cashbox_id)}))"
                            )
                        )
                    else:
                        filters_list.append(
                            (
                                f"(payments.paybox IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = payments.cashbox) OR "
                                f"payments.paybox_to IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = payments.cashbox))"
                            )
                        )
                else:
                    if cashbox_id is not None:
                        filters_list.append(
                            (
                                f"payments.paybox IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = {int(cashbox_id)})"
                            )
                        )
                    else:
                        filters_list.append(
                            (
                                f"payments.paybox IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = payments.cashbox)"
                            )
                        )
        elif filter == "paybox_to":
            if value:
                if cashbox_id is not None:
                    filters_list.append(
                        (
                            f"payments.paybox_to IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = {int(cashbox_id)})"
                        )
                    )
                else:
                    filters_list.append(
                        (
                            f"payments.paybox_to IN (SELECT payboxes.id FROM payboxes WHERE payboxes.name ILIKE '%{value}%' AND payboxes.cashbox = payments.cashbox)"
                        )
                    )
        elif filter == "payment_type":
            if value:
                if value in (
                    PaymentType.incoming,
                    PaymentType.outgoing,
                    PaymentType.transfer,
                ):
                    filters_list.append((f"payments.type = '{value}'"))
        elif filter == "external_id":
            if value:
                filters_list.append((f"payments.external_id ILIKE '%{value}%'"))
        elif filter == "relship":
            if value:
                if value == "parents":
                    # filters_list.append(table.c.parent_id == None)
                    filters_list.append(("payments.parent_id IS NULL"))
                elif value == "childs":
                    # filters_list.append(table.c.parent_id != None)
                    filters_list.append(("payments.parent_id IS NOT NULL"))

    datefrom = None
    dateto = None

    timezone_str = filters_dict.get("timezone", "UTC")
    try:
        timezone = pytz.timezone(timezone_str)
    except pytz.exceptions.UnknownTimeZoneError:
        timezone = pytz.UTC

    if filters_dict["datefrom"]:
        try:
            date_obj = datetime.strptime(filters_dict["datefrom"], "%d-%m-%Y")
            date_obj = timezone.localize(date_obj)
            datefrom = date_obj.astimezone(pytz.UTC).timestamp()
        except (ValueError, TypeError):
            datefrom = None

    if filters_dict["dateto"]:
        try:
            date_obj = datetime.strptime(filters_dict["dateto"], "%d-%m-%Y").replace(
                hour=23, minute=59, second=59
            )
            date_obj = timezone.localize(date_obj)
            dateto = date_obj.astimezone(pytz.UTC).timestamp()
        except (ValueError, TypeError):
            dateto = None

    if datefrom and not dateto:
        # filters_list.append(table.c.date >= datefrom)
        filters_list.append((f"payments.date >= {int(datefrom)}"))

    if not datefrom and dateto:
        # filters_list.append(table.c.date <= dateto)
        filters_list.append((f"payments.date <= {int(dateto)}"))

    if datefrom and dateto:
        filters_list.append(
            (f"payments.date >= {int(datefrom)} AND payments.date <= {int(dateto)}")
        )

    if filters_list:
        return f"AND {' AND '.join(filters_list)}"
    else:
        return ""


def get_filters_analytics(filters) -> str:
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "paybox_id":
            if value:
                values: list[str] = value.split(",")
                expression = ""
                for val in values:
                    if val.isdigit():
                        expression = expression + val + ", "
                    else:
                        raise ValueError
                expression = expression.rstrip(", ")
                filters_list.append(
                    f"""
                    AND payments.paybox IN
                    (SELECT payboxes.id FROM payboxes
                    WHERE payboxes.id IN ({expression}) AND payboxes.cashbox = payments.cashbox)
                """
                )
        elif filter == "status":
            if value:
                values: list[str] = value.split(",")
                expression = ""
                for val in values:
                    if val in ("true", "false"):
                        expression = expression + val + ", "
                    else:
                        raise ValueError
                expression = expression.rstrip(", ")
                filters_list.append(f" AND payments.status IN ({expression})")

    datefrom = filters_dict["datefrom"]
    dateto = filters_dict["dateto"]

    if datefrom:
        filters_list.append(f" AND payments.date >= {int(datefrom)}")

    if dateto:
        filters_list.append(f" AND payments.date <= {int(dateto)}")

    return " ".join(filters_list)


def get_filters_transactions(table, filters):
    filters_list = []
    filters_dict = filters.dict()

    def _periods_filter(period_type):
        datefrom = filters_dict.get(f"{period_type}_from")
        dateto = filters_dict.get(f"{period_type}_to")

        if datefrom and not dateto:
            filters_list.append(table.c.dated >= datetime.fromtimestamp(datefrom))

        if not datefrom and dateto:
            filters_list.append(table.c.dated <= datetime.fromtimestamp(dateto))

        if datefrom and dateto:
            filters_list.append(
                and_(
                    table.c.dated >= datetime.fromtimestamp(datefrom),
                    table.c.dated <= datetime.fromtimestamp(dateto),
                )
            )

    _periods_filter("dated")

    updated_at_from = filters_dict.get("updated_at__gte")
    if updated_at_from:
        filters_list.append(
            table.c.updated_at >= datetime.fromtimestamp(updated_at_from)
        )

    updated_at_to = filters_dict.get("updated_at__lte")
    if updated_at_to:
        filters_list.append(table.c.updated_at <= datetime.fromtimestamp(updated_at_to))

    for filter, value in filters_dict.items():
        if filter == "type":
            if value:
                filters_list.append(table.c.type == value)
        if filter == "loyality_card_number":
            if value:
                filters_list.append(
                    cast(table.c.loyality_card_number, String).ilike(
                        f"%{str(value)[1:]}%"
                    )
                )
        if filter == "amount":
            if value:
                filters_list.append(table.c.amount == value)
        if filter == "tags":
            if value:
                filters_list.append(table.c.tags.ilike(f"%{value}%"))
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))
        if filter == "description":
            if value:
                filters_list.append(table.c.description.ilike(f"%{value}%"))

    return filters_list


def get_filters_cards(table, filters):
    filters_list = []
    filters_dict = filters.dict()

    and_conditions = []
    if filters_dict.get("start_period_from"):
        and_conditions.append(
            table.c.start_period
            >= datetime.fromtimestamp(filters_dict.get("start_period_from"))
        )
    if filters_dict.get("start_period_to"):
        and_conditions.append(
            table.c.start_period
            <= datetime.fromtimestamp(filters_dict.get("start_period_to"))
        )
    if filters_dict.get("end_period_from"):
        and_conditions.append(
            table.c.end_period
            >= datetime.fromtimestamp(filters_dict.get("end_period_from"))
        )
    if filters_dict.get("end_period_to"):
        and_conditions.append(
            table.c.end_period
            <= datetime.fromtimestamp(filters_dict.get("end_period_to"))
        )
    if filters_dict.get("created_at_from"):
        and_conditions.append(
            table.c.created_at
            >= datetime.fromtimestamp(filters_dict.get("created_at_from"))
        )
    if filters_dict.get("created_at_to"):
        and_conditions.append(
            table.c.created_at
            <= datetime.fromtimestamp(filters_dict.get("created_at_to"))
        )
    if filters_dict.get("updated_at_from"):
        and_conditions.append(
            table.c.updated_at
            >= datetime.fromtimestamp(filters_dict.get("updated_at_from"))
        )
    if filters_dict.get("updated_at_to"):
        and_conditions.append(
            table.c.updated_at
            <= datetime.fromtimestamp(filters_dict.get("updated_at_to"))
        )
    filters_list.append(and_(*and_conditions))

    for filter, value in filters_dict.items():
        if filter == "card_number":
            if value:
                filters_list.append(
                    cast(table.c.card_number, String).ilike(f"%{value}%")
                )
        if filter == "balance":
            if value:
                filters_list.append(table.c.balance == value)
        if filter == "tags":
            if value:
                filters_list.append(table.c.tags.ilike(f"%{value}%"))
        if filter == "income":
            if value:
                filters_list.append(table.c.income == value)
        if filter == "outcome":
            if value:
                filters_list.append(table.c.outcome == value)
        if filter == "cashback_percent":
            if value:
                filters_list.append(table.c.cashback_percent == value)
        if filter == "minimal_checque_amount":
            if value:
                filters_list.append(table.c.minimal_checque_amount == value)
        if filter == "max_percentage":
            if value:
                filters_list.append(table.c.max_percentage == value)
        if filter == "status_card":
            if value:
                filters_list.append(table.c.status_card == value)

    return filters_list


def get_filters_pboxes(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "external_id":
            if value:
                filters_list.append(table.c.external_id.ilike(f"%{value}%"))
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))

    return filters_list


def get_filters_projects(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "external_id":
            if value:
                filters_list.append(table.c.external_id.ilike(f"%{value}%"))
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))

    return filters_list


def get_filters_articles(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))
        if filter == "dc":
            if value:
                filters_list.append(table.c.dc == value)
    return filters_list


def get_filters_users(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "external_id":
            if value:
                filters_list.append(table.c.external_id.ilike(f"%{value}%"))

    return filters_list


def get_filters_ca(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "name":
            if value:
                filters_list.append(table.c.name.ilike(f"%{value}%"))
        elif filter == "inn":
            if value:
                filters_list.append(table.c.inn.ilike(r"%{}%".format(value)))
        elif filter == "phone":
            if value:
                normalized_search_phone = clear_phone_number(value)
                if normalized_search_phone:
                    normalized_phone_in_db = func.regexp_replace(
                        table.c.phone, r"[^\d]", "", "g"
                    )
                    filters_list.append(
                        normalized_phone_in_db.ilike(f"%{normalized_search_phone}%")
                    )
                else:
                    filters_list.append(table.c.phone.ilike(r"%{}%".format(value)))
        elif filter == "external_id":
            if value:
                filters_list.append(table.c.external_id.ilike(r"%{}%".format(value)))

    return filters_list


def get_filters_cheques(table, filters):
    filters_list = []
    filters_dict = filters.dict()
    for filter, value in filters_dict.items():
        if filter == "user":
            if value:
                filters_list.append(table.c.user == value)

        date_from = filters_dict["datefrom"]
        date_to = filters_dict["dateto"]

        if date_from:
            filters_list.append(table.c.created_at >= int(date_from))

        if date_to:
            filters_list.append(table.c.created_at <= int(date_to))

    return filters_list


def raise_wrong_token():
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


def raise_bad_request(message: str):
    raise HTTPException(status_code=400, detail=message)


async def add_nomenclature_count(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        q = docs_sales_goods.select().where(
            docs_sales_goods.c.docs_sales_id == instance["id"]
        )
        goods = await database.fetch_all(q)

        if goods:
            instance["nomenclature_count"] = len(goods)
            instance["doc_discount"] = round(
                sum(
                    [
                        (0 if not good.sum_discounted else good.sum_discounted)
                        for good in goods
                    ]
                ),
                2,
            )
        else:
            instance["nomenclature_count"] = 0
            instance["doc_discount"] = 0

        return instance


async def add_nomenclature_name_to_goods(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        q = nomenclature.select().where(nomenclature.c.id == instance["nomenclature"])
        nomenclature_db = await database.fetch_one(q)

        if instance.get("unit"):
            q = units.select().where(units.c.id == instance.get("unit"))
            unit_db = await database.fetch_one(q)

            if unit_db:
                instance["unit_name"] = unit_db.convent_national_view

        if nomenclature_db:
            instance["nomenclature_name"] = nomenclature_db.name
        else:
            instance["nomenclature_name"] = ""

        return instance


async def add_docs_sales_settings(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        settings = await database.fetch_one(
            docs_sales_settings.select().where(
                docs_sales_settings.c.id == instance["settings"]
            )
        )

        instance["settings"] = settings

        return instance


async def raschet_oplat(instance: Optional[Record]) -> Optional[dict]:
    if instance is None:
        return None

    instance = dict(instance)

    proxyes_q = entity_to_entity.select().where(
        entity_to_entity.c.cashbox_id == instance["cashbox"],
        entity_to_entity.c.from_id == instance["id"],
    )
    proxyes = await database.fetch_all(proxyes_q)

    payment_ids = [
        proxy.to_id
        for proxy in proxyes
        if proxy.from_entity == 7 and proxy.to_entity == 5
    ]
    transaction_ids = [
        proxy.to_id
        for proxy in proxyes
        if proxy.from_entity == 7 and proxy.to_entity == 6
    ]

    paid_rubles = 0
    paid_loyality = 0

    if payment_ids:
        q_payment = payments.select().where(
            payments.c.id.in_(payment_ids),
            payments.c.cashbox == instance["cashbox"],
            payments.c.status == True,
            payments.c.is_deleted == False,
        )
        payments_data = await database.fetch_all(q_payment)
        paid_rubles = sum(payment.amount for payment in payments_data)

    if transaction_ids:
        q_trans = loyality_transactions.select().where(
            loyality_transactions.c.id.in_(transaction_ids),
            loyality_transactions.c.cashbox == instance["cashbox"],
            loyality_transactions.c.status == True,
            loyality_transactions.c.is_deleted == False,
        )
        transactions_data = await database.fetch_all(q_trans)
        paid_loyality = sum(trans.amount for trans in transactions_data)

    instance["paid_rubles"] = round(paid_rubles, 2)
    instance["paid_loyality"] = round(paid_loyality, 2)
    instance["paid_doc"] = round(paid_loyality + paid_rubles, 2)

    return instance


async def nomenclature_unit_id_to_name(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        q = units.select().where(units.c.id == instance.get("unit"))
        unit_db = await database.fetch_one(q)

        if unit_db:
            instance["unit_name"] = unit_db.convent_national_view

        return instance


def datetime_to_timestamp(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)
        if instance.get("start_period"):
            instance["start_period"] = int(instance["start_period"].timestamp())
        if instance.get("end_period"):
            instance["end_period"] = int(instance["end_period"].timestamp())

        if instance.get("dead_at"):
            instance["dead_at"] = int(instance["dead_at"].timestamp())
        if instance.get("dated"):
            try:
                instance["dated"] = int(instance["dated"].timestamp())
            except AttributeError:
                pass

        if instance.get("created_at"):
            instance["created_at"] = int(instance["created_at"].timestamp())
        if instance.get("updated_at"):
            instance["updated_at"] = int(instance["updated_at"].timestamp())
        return instance


def rem_owner_is_deleted(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)
        if instance.get("owner") is not None:
            del instance["owner"]
        if instance.get("is_deleted") is not None:
            del instance["is_deleted"]

        return instance


def add_status(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)
        instance["data"] = {"status": "success"}

        return instance


async def get_user_by_token(token: str) -> Record:
    query = users_cboxes_relation.select().where(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query)
    if not user or not user.status:
        raise_wrong_token()
    return user


async def get_any_cashbox_user_id(cashbox_id: int) -> Optional[int]:
    """Возвращяет id какого-то объекта relation_tg_cashboxes с данным cashbox_id;
    испольщуется для назначения owner файлу, получаемому через телеграмм чат"""
    query = (
        users_cboxes_relation.select()
        .where(users_cboxes_relation.c.cashbox_id == cashbox_id)
        .limit(1)
    )
    user = await database.fetch_one(query)
    if not user or not user.status:
        return None
    return user.id


async def check_user_permission(
    user_id: int,
    cashbox_id: int,
    section: str,
    paybox_id: int = None,
    need_edit: bool = False,
) -> bool:
    """
    Проверка прав пользователя на доступ к разделу/счету

    Args:
        user_id: ID пользователя
        cashbox_id: ID кассы
        section: Название раздела (payments, pboxes и т.д.)
        paybox_id: ID счета (опционально)
        need_edit: Требуется ли право на редактирование

    Returns:
        bool: Есть ли у пользователя требуемые права
    """
    user_query = users_cboxes_relation.select().where(
        and_(
            users_cboxes_relation.c.id == user_id,
            users_cboxes_relation.c.cashbox_id == cashbox_id,
        )
    )
    user = await database.fetch_one(user_query)
    if user and user.is_owner:
        return True

    query = user_permissions.select().where(
        and_(
            user_permissions.c.user_id == user_id,
            user_permissions.c.cashbox_id == cashbox_id,
            user_permissions.c.section == section,
            user_permissions.c.can_view == True,
        )
    )

    if need_edit:
        query = query.where(user_permissions.c.can_edit == True)

    if paybox_id:
        query = query.where(
            or_(
                user_permissions.c.paybox_id.is_(None),
                user_permissions.c.paybox_id == paybox_id,
            )
        )

    permission = await database.fetch_one(query)
    return bool(permission)


async def hide_balance_for_non_admin(user, data):
    """
    Скрывает баланс счета для обычных пользователей (не админов)

    Args:
        user: Объект пользователя из таблицы users_cboxes_relation
        data: Данные счетов (список или один счет)

    Returns:
        Модифицированные данные счетов
    """
    if not user.is_owner:
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and "balance" in item:
                    item["balance"] = None
                    if "name" in item and " (" in item["name"]:
                        item["name"] = item["name"].split(" (")[0]
        else:
            if isinstance(data, dict) and "balance" in data:
                data["balance"] = None
                if "name" in data and " (" in data["name"]:
                    data["name"] = data["name"].split(" (")[0]

    return data


async def get_entity_by_id(entity: Table, idx: int, cashbox: int) -> Record:
    """Returns entity from db, filtered by owner and is_deleted fields"""

    query = entity.select().where(
        entity.c.id == idx,
        entity.c.cashbox == cashbox,
        entity.c.is_deleted.is_not(True),
    )
    entity_db = await database.fetch_one(query)

    if not entity_db:
        raise HTTPException(
            status_code=404, detail=f"У вас нет {entity.name.rstrip('s')} с таким id."
        )

    return entity_db


async def get_entity_by_id_and_created_by(
    entity: Table, idx: int, created_by: int
) -> Record:
    """Returns entity from db, filtered by owner and is_deleted fields"""

    query = entity.select().where(
        entity.c.id == idx,
        entity.c.created_by_id == created_by,
        entity.c.is_deleted.is_not(True),
    )
    entity_db = await database.fetch_one(query)

    if not entity_db:
        raise HTTPException(
            status_code=404, detail=f"У вас нет {entity.name.rstrip('s')} с таким id."
        )

    return entity_db


async def get_entity_by_id_cashbox(entity: Table, idx: int, cashbox_id: int) -> Record:
    """Returns entity from db, filtered by cashbox_id and is_deleted fields"""

    query = entity.select().where(
        entity.c.id == idx,
        entity.c.cashbox == cashbox_id,
        entity.c.is_deleted.is_not(True),
    )
    entity_db = await database.fetch_one(query)

    if not entity_db:
        raise HTTPException(
            status_code=404, detail=f"У вас нет {entity.name.rstrip('s')} с таким id."
        )

    return entity_db


async def contr_org_ids_to_name(instance: Optional[Record]) -> Optional[dict]:
    if instance is not None:
        instance = dict(instance)

        contr_id = instance["contragent_id"]
        org_id = instance["organization_id"]

        query = contragents.select().where(contragents.c.id == contr_id)
        contr = await database.fetch_one(query)

        query = organizations.select().where(organizations.c.id == org_id)
        org = await database.fetch_one(query)

        instance["contragent"] = contr.name
        instance["organization"] = org.short_name if org else ""

        return instance


async def check_contragent_exists(idx, cashbox_id):
    query = contragents.select().where(
        contragents.c.id == idx, contragents.c.cashbox == cashbox_id
    )
    if not await database.fetch_one(query):
        raise HTTPException(
            status_code=403,
            detail="Введенный контрагент не принадлежит вам или не существует!",
        )
    return True


async def check_unit_exists(unit_id):
    query = units.select().where(units.c.id == unit_id)
    if not await database.fetch_one(query):
        raise HTTPException(
            status_code=403,
            detail="Единицы измерения с этим id не существует!",
        )
    return True


async def check_function_exists(name: str):
    query = entity_or_function.select().where(entity_or_function.c.name == name)
    if not await database.fetch_one(query):
        raise HTTPException(
            status_code=404,
            detail="Функция не существует!",
        )
    return True


async def check_entity_exists(entity: Table, idx, user_id=None):
    query = entity.select().where(entity.c.id == idx)
    if not await database.fetch_one(query):
        raise HTTPException(
            status_code=404,
            detail=f"{entity.name.rstrip('s')} не существует!",
        )
    return True


async def check_period_blocked(organization_id: int, date: int, exceptions: list[str]):
    if organization_id is not None and date:
        query = fifo_settings.select().where(
            fifo_settings.c.organization_id == organization_id,
            fifo_settings.c.blocked_date >= date,
        )
        if await database.fetch_one(query):
            exceptions.append(
                f"Период закрыт для организации {organization_id} на указанную дату."
            )
            return False
    return True


def clear_phone_number(phone_number) -> Union[int, None]:
    """Универсальная очистка телефонного номера"""
    # Защита от None и пустых значений
    if phone_number is None:
        return None

    # Если уже int - возвращаем как есть
    if isinstance(phone_number, int):
        return phone_number

    # Конвертируем в строку
    if not isinstance(phone_number, str):
        phone_number = str(phone_number)

    # Защита от пустой строки
    if not phone_number or phone_number.strip() == "":
        return None

    # Убираем + в начале
    if phone_number.startswith("+"):
        phone_number = phone_number[1:]

    try:
        # Оставляем только цифры
        return "".join(filter(str.isdigit, phone_number))
    except (ValueError, TypeError):
        return None


async def get_statement(statement_id: str, account_id: str, access_token: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://enter.tochka.com/uapi/open-banking/v1.0/accounts/{account_id}/statements/{statement_id}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}",
            },
        ) as resp:
            try:
                init_statement_json = await resp.json()
            except:
                init_statement_json = {"Data": {"Statement": {"status": "error"}}}
        await session.close()
    return init_statement_json


async def init_statement(statement_data: dict, access_token: str):
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://enter.tochka.com/uapi/open-banking/v1.0/statements",
            json={
                "Data": {
                    "Statement": {
                        "accountId": statement_data.get("accountId"),
                        "startDateTime": statement_data.get("startDateTime"),
                        "endDateTime": statement_data.get("endDateTime"),
                    }
                }
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}",
            },
        ) as resp:
            try:
                init_statement_json = await resp.json()
            except:
                init_statement_json = {"Data": {"Statement": {"status": "error"}}}
        await session.close()
    return init_statement_json


async def add_delivery_info_to_doc(doc: dict) -> dict:
    delivery_info_query = docs_sales_delivery_info.select().where(
        docs_sales_delivery_info.c.docs_sales_id == doc.get("id")
    )
    delivery_info = await database.fetch_one(delivery_info_query)
    if delivery_info:
        doc["delivery_info"] = {
            "address": delivery_info.get("address"),
            "delivery_date": (
                delivery_info["delivery_date"].timestamp()
                if delivery_info.get("delivery_date")
                else None
            ),
            "delivery_price": delivery_info["delivery_price"],
            "recipient": delivery_info.get("recipient"),
            "note": delivery_info.get("note"),
        }
    return doc


async def check_article_exists(name: str, user_cashbox_id: str, dc_type: str):
    check_query = articles.select().where(
        and_(
            articles.c.name == name,
            articles.c.cashbox == user_cashbox_id,
            articles.c.dc == dc_type,
        )
    )
    article_exists = await database.fetch_all(check_query)
    return True if article_exists else False


async def create_entity_hash(table: Table, table_hash: Table, idx: int):
    query = (
        table.select()
        .with_only_columns(table.c.id, table.c.created_at, table.c.updated_at)
        .where(table.c.id == idx)
    )
    entity = await database.fetch_one(query)
    data = str(
        {
            "type": table.name,
            "id": entity.id,
            "created_at": entity.created_at,
            "updated_at": entity.updated_at,
        }
    )

    hash_string = f"{table.name}_" + str(hashlib.md5(data.encode("utf-8")).hexdigest())
    hash_data = {
        "hash": hash_string,
        f"{table.name}_id": entity.id,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    insert_query = table_hash.insert().values(hash_data)
    await database.execute(insert_query)


async def update_entity_hash(table: Table, table_hash: Table, entity):
    data = str(
        {
            "type": table.name,
            "id": entity.id,
            "created_at": entity.created_at,
            "updated_at": entity.updated_at,
        }
    )

    # Используем новые префиксы: nm_ для товаров, wh_ для складов
    if table.name == "nomenclature":
        hash_string = "nm_" + str(hashlib.md5(data.encode("utf-8")).hexdigest())
    else:  # warehouses
        hash_string = "wh_" + str(hashlib.md5(data.encode("utf-8")).hexdigest())

    hash_data = {
        "hash": hash_string,
        f"{table.name}_id": entity.id,
        "updated_at": datetime.now(),
    }
    if table.name == "nomenclature":
        update_query = (
            table_hash.update()
            .where(table_hash.c.nomenclature_id == entity.id)
            .values(hash_data)
        )
    else:
        update_query = (
            table_hash.update()
            .where(table_hash.c.warehouses_id == entity.id)
            .values(hash_data)
        )
    await database.execute(update_query)


def sanitize_float(value):
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
    return value


def deep_sanitize(obj):
    if isinstance(obj, dict):
        return {k: deep_sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deep_sanitize(v) for v in obj]
    else:
        return sanitize_float(obj)


def coerce_value(v: str):
    """Приведение строк из query к bool/int/float/list/json."""
    if v is None:
        return None
    s = v.strip()
    # bool
    if s.lower() in ("true", "false"):
        return s.lower() == "true"
    # int
    if s.isdigit():
        try:
            return int(s)
        except:
            pass
    # float (включая отрицательные)
    try:
        if "." in s or (s.startswith("-") and s[1:].replace(".", "", 1).isdigit()):
            return float(s)
    except:
        pass
    # JSON
    if (s.startswith("{") and s.endswith("}")) or (
        s.startswith("[") and s.endswith("]")
    ):
        try:
            return json.loads(s)
        except:
            pass

    return s


def build_filters(table: Table, filter_schema: Any) -> List[ColumnElement]:
    """
    Преобразует Pydantic-схему в список SQLAlchemy Core фильтров.
    Возвращает список ColumnElement, пригодных для .where(*filters)
    """
    operators = {
        "eq": lambda c, v: c == v,
        "ne": lambda c, v: c != v,
        "lt": lambda c, v: c < v,
        "lte": lambda c, v: c <= v,
        "gt": lambda c, v: c > v,
        "gte": lambda c, v: c >= v,
        "in": lambda c, v: c.in_(v if isinstance(v, list) else str(v).split(",")),
    }

    filters = []
    if isinstance(filter_schema, dict):
        filters_data = filter_schema.copy()
    else:
        filters_data = filter_schema.dict(exclude_none=True)
    for key, value in filters_data.items():
        if "__" in key:
            field_name, op = key.split("__", 1)
        else:
            field_name, op = key, "eq"

        if field_name not in table.c:
            raise ValueError(f"Таблица {table.name} не содержит столбца '{field_name}'")

        column = table.c[field_name]
        if op not in operators:
            raise ValueError(f"Неподдерживаемый оператор '{op}' в фильтре '{key}'")

        filters.append(operators[op](column, value))

    return filters


def build_sql_filters(filter_schema: Any) -> str:
    """
    Генерирует строку SQL-фильтров (часть WHERE)
    """
    filters = []
    for key, value in filter_schema.dict(exclude_none=True).items():
        if "__" in key:
            field_name, op = key.split("__", 1)
        else:
            field_name, op = key, "eq"

        if isinstance(value, str):
            value = f"'{value}'"
        elif isinstance(value, list):
            value = "(" + ", ".join(f"'{v}'" for v in value) + ")"

        # SQL маппинг операторов
        op_map = {
            "eq": "=",
            "ne": "!=",
            "lt": "<",
            "lte": "<=",
            "gt": ">",
            "gte": ">=",
            "in": "IN",
        }

        sql_op = op_map.get(op)
        if not sql_op:
            raise ValueError(f"Unsupported operator '{op}'")

        filters.append(f"AND payments.{field_name} {sql_op} {value}")

    return " ".join(filters)
