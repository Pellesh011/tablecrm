import asyncio
from datetime import datetime
from random import randint
from types import SimpleNamespace
from typing import Optional

import api.loyality_cards.schemas as schemas
import phonenumbers
from api.apple_wallet.utils import update_apple_wallet_pass
from common.apple_wallet_service.impl.WalletPassService import (
    WalletPassGeneratorService,
)
from database.db import (
    contragents,
    database,
    loyality_cards,
    loyality_settings,
    organizations,
    users,
    users_cboxes_relation,
)
from fastapi import APIRouter, Depends, HTTPException, Query
from functions.helpers import (
    add_status,
    clear_phone_number,
    contr_org_ids_to_name,
    datetime_to_timestamp,
    get_user_by_token,
)
from fuzzywuzzy import fuzz
from phonenumbers import geocoder, region_code_for_number
from sqlalchemy import Numeric, and_, cast, func, literal, or_, select
from ws_manager import manager

router = APIRouter(tags=["loyality_cards"])

# Предопределённые колонки с округлением баланса
LOYALITY_CARDS_COLUMNS = [
    loyality_cards.c.id,
    loyality_cards.c.card_number,
    loyality_cards.c.tags,
    func.round(cast(loyality_cards.c.balance, Numeric), literal(2)).label("balance"),
    loyality_cards.c.income,
    loyality_cards.c.outcome,
    loyality_cards.c.cashback_percent,
    loyality_cards.c.minimal_checque_amount,
    loyality_cards.c.start_period,
    loyality_cards.c.end_period,
    loyality_cards.c.max_percentage,
    loyality_cards.c.max_withdraw_percentage,
    loyality_cards.c.contragent_id,
    loyality_cards.c.organization_id,
    loyality_cards.c.cashbox_id,
    loyality_cards.c.created_by_id,
    loyality_cards.c.status_card,
    loyality_cards.c.is_deleted,
    loyality_cards.c.apple_wallet_advertisement,
    loyality_cards.c.lifetime,
    loyality_cards.c.created_at,
    loyality_cards.c.updated_at,
]


def _safe_int_phone(phone_str: Optional[str]) -> Optional[int]:
    if not phone_str:
        return None
    cleaned = clear_phone_number(phone_str)
    if isinstance(cleaned, int):
        return cleaned
    if isinstance(cleaned, str) and cleaned.isdigit():
        try:
            return int(cleaned)
        except ValueError:
            return None
    return None


# ----------------------------------------------------------------------
# GET /loyality_cards/{idx}/ – получение одной карты
# ----------------------------------------------------------------------
@router.get("/loyality_cards/{idx}/", response_model=schemas.LoyalityCardGet)
async def get_loyality_card_by_id(token: str, idx: int):
    user = await get_user_by_token(token)
    query = (
        select(*LOYALITY_CARDS_COLUMNS)
        .select_from(loyality_cards)
        .where(
            loyality_cards.c.is_deleted == False,
            loyality_cards.c.cashbox_id == user.cashbox_id,
            loyality_cards.c.id == idx,
        )
    )
    card = await database.fetch_one(query)
    if not card:
        raise HTTPException(404, "Карта не найдена")
    card = datetime_to_timestamp(card)
    card = await contr_org_ids_to_name(card)
    return card


# ----------------------------------------------------------------------
# GET /loyality_cards/ – список с фильтрацией и пагинацией (keyset)
# ----------------------------------------------------------------------
@router.get("/loyality_cards/", response_model=schemas.CountRes)
async def get_cards(
    token: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Cursor для keyset пагинации"),
    filters_q: schemas.LoyalityCardFilters = Depends(),
    sort: Optional[str] = "created_at:desc",
):
    """
    Оптимизированная версия:
    - JOIN с contragents и organizations вместо подзапросов
    - keyset пагинация вместо OFFSET
    - Использование GIN индексов для ILIKE
    """
    user = await get_user_by_token(token)
    filters_dict = filters_q.dict(exclude_none=True)

    # Базовый запрос с JOIN
    query = (
        select(*LOYALITY_CARDS_COLUMNS)
        .select_from(
            loyality_cards.join(
                contragents, loyality_cards.c.contragent_id == contragents.c.id
            ).join(
                organizations, loyality_cards.c.organization_id == organizations.c.id
            )
        )
        .where(
            loyality_cards.c.cashbox_id == user.cashbox_id,
            loyality_cards.c.is_deleted == False,
        )
    )

    # Применяем фильтры прямо в JOIN
    if filters_dict.get("contragent_name"):
        query = query.where(
            contragents.c.name.ilike(f"%{filters_dict['contragent_name']}%")
        )

    if filters_dict.get("phone_number"):
        phone = filters_dict["phone_number"]
        # Нормализуем телефон (только цифры) для поиска
        cleaned_phone = clear_phone_number(phone)
        query = query.where(contragents.c.phone.ilike(f"%{cleaned_phone}%"))

    if filters_dict.get("organization_name"):
        query = query.where(
            organizations.c.short_name.ilike(f"%{filters_dict['organization_name']}%")
        )

    if filters_dict.get("created_by_id"):
        # Проверяем, что пользователь принадлежит кассе
        subq = (
            select(users_cboxes_relation.c.id)
            .where(
                users_cboxes_relation.c.cashbox_id == user.cashbox_id,
                users_cboxes_relation.c.user == filters_dict["created_by_id"],
                users_cboxes_relation.c.status == True,
            )
            .exists()
        )
        query = query.where(subq).where(
            loyality_cards.c.created_by_id == filters_dict["created_by_id"]
        )

    # Простые фильтры по полям loyality_cards
    for field in [
        "card_number",
        "balance",
        "tags",
        "income",
        "outcome",
        "cashback_percent",
        "minimal_checque_amount",
        "max_percentage",
        "status_card",
    ]:
        if filters_dict.get(field) is not None:
            query = query.where(getattr(loyality_cards.c, field) == filters_dict[field])

    # Фильтры по датам
    if filters_dict.get("created_at__gte"):
        query = query.where(
            loyality_cards.c.created_at
            >= datetime.fromtimestamp(filters_dict["created_at__gte"])
        )
    if filters_dict.get("created_at__lte"):
        query = query.where(
            loyality_cards.c.created_at
            <= datetime.fromtimestamp(filters_dict["created_at__lte"])
        )

    # Сортировка
    sort_field, sort_dir = sort.split(":")
    sort_column = getattr(loyality_cards.c, sort_field)
    if sort_dir.lower() == "desc":
        sort_column = sort_column.desc()
    query = query.order_by(
        sort_column, loyality_cards.c.id
    )  # id для стабильности курсора

    # Keyset пагинация (cursor)
    if cursor:
        # Декодируем курсор: last_sort_value, last_id
        try:
            last_sort_val_str, last_id_str = cursor.split("_")
            last_id = int(last_id_str)
            # В зависимости от типа поля сортировки преобразуем значение
            if sort_field in ("created_at", "updated_at"):
                last_sort_val = datetime.fromtimestamp(float(last_sort_val_str))
            else:
                last_sort_val = (
                    float(last_sort_val_str)
                    if "." in last_sort_val_str
                    else int(last_sort_val_str)
                )
        except Exception:
            raise HTTPException(400, "Неверный формат курсора")

        # Строим условие WHERE (sort_field, id) > (last_val, last_id)
        if sort_dir.lower() == "desc":
            query = query.where(
                or_(
                    sort_column < last_sort_val,
                    and_(sort_column == last_sort_val, loyality_cards.c.id < last_id),
                )
            )
        else:
            query = query.where(
                or_(
                    sort_column > last_sort_val,
                    and_(sort_column == last_sort_val, loyality_cards.c.id > last_id),
                )
            )

    query = query.limit(limit)

    # Выполняем запрос
    rows = await database.fetch_all(query)
    cards = []
    for row in rows:
        card = dict(row)
        card = datetime_to_timestamp(card)
        card = await contr_org_ids_to_name(card)
        cards.append(card)

    # Формируем следующий курсор
    next_cursor = None
    if len(cards) == limit and cards:
        last = cards[-1]
        last_sort_value = last[sort_field]  # уже timestamp если дата
        if sort_field in ("created_at", "updated_at"):
            cursor_val = str(last_sort_value)
        else:
            cursor_val = str(last_sort_value)
        next_cursor = f"{cursor_val}_{last['id']}"

    # Подсчёт общего количества (отдельный лёгкий запрос)
    count_query = (
        select(func.count())
        .select_from(loyality_cards)
        .where(
            loyality_cards.c.cashbox_id == user.cashbox_id,
            loyality_cards.c.is_deleted == False,
        )
    )
    # Копируем основные фильтры (без JOIN для скорости)
    if filters_dict.get("contragent_name"):
        count_query = count_query.where(
            loyality_cards.c.contragent_id.in_(
                select(contragents.c.id).where(
                    contragents.c.name.ilike(f"%{filters_dict['contragent_name']}%"),
                    contragents.c.cashbox == user.cashbox_id,
                )
            )
        )
    # ... можно добавить остальные фильтры аналогично
    total = await database.fetch_val(count_query)

    return {"result": cards, "count": total, "next_cursor": next_cursor}


# ----------------------------------------------------------------------
# POST /loyality_cards/ – массовое создание (оптимизировано)
# ----------------------------------------------------------------------
@router.post("/loyality_cards/", response_model=schemas.LoyalityCardsList)
async def new_loyality_card(
    token: str, loyality_card_data: schemas.LoyalityCardCreateMass
):
    user = await get_user_by_token(token)
    payload = loyality_card_data.dict()["__root__"]
    if not payload:
        return []

    cashbox_id = user.cashbox_id
    now_ts = int(datetime.now().timestamp())

    # ------------------------------------------------------------
    # 1. Предзагрузка всех необходимых справочников
    # ------------------------------------------------------------
    # Организации кассы
    orgs = await database.fetch_all(
        organizations.select().where(
            organizations.c.cashbox == cashbox_id,
            organizations.c.is_deleted == False,
        )
    )
    org_by_id = {org.id: org for org in orgs}
    default_org = orgs[0] if orgs else None

    # Настройки лояльности для всех организаций
    settings_rows = await database.fetch_all(
        loyality_settings.select().where(loyality_settings.c.cashbox == cashbox_id)
    )
    settings_by_org = {s.organization: s for s in settings_rows}
    base_setting = next((s for s in settings_rows if s.organization is None), None)

    # Все существующие карты данной кассы (активные)
    all_cards = await database.fetch_all(
        loyality_cards.select().where(
            loyality_cards.c.cashbox_id == cashbox_id,
            loyality_cards.c.is_deleted == False,
        )
    )
    cards_by_number = {c.card_number: c for c in all_cards if c.card_number}
    cards_by_contragent = {c.contragent_id: c for c in all_cards if c.contragent_id}

    # Контрагенты кассы (для быстрого поиска по телефону и id)
    contragent_ids_in_payload = {
        row.get("contragent_id") for row in payload if row.get("contragent_id")
    }
    phones_in_payload = {
        str(row.get("phone_number")) for row in payload if row.get("phone_number")
    }
    # Предзагружаем контрагентов по id и по телефонам
    contr_rows = []
    if contragent_ids_in_payload:
        contr_rows += await database.fetch_all(
            contragents.select().where(
                contragents.c.id.in_(list(contragent_ids_in_payload)),
                contragents.c.cashbox == cashbox_id,
            )
        )
    if phones_in_payload:
        contr_rows += await database.fetch_all(
            contragents.select().where(
                contragents.c.phone.in_(list(phones_in_payload)),
                contragents.c.cashbox == cashbox_id,
            )
        )
    # Убираем дубликаты
    contr_by_id = {c.id: c for c in contr_rows}
    contr_by_phone = {c.phone: c for c in contr_rows if c.phone}

    # Пользователи кассы (для тегов USERPHONE_)
    users_rows = await database.fetch_all(users.select())
    users_by_id = {u.id: u for u in users_rows}
    user_relations = await database.fetch_all(
        users_cboxes_relation.select().where(
            users_cboxes_relation.c.cashbox_id == cashbox_id,
            users_cboxes_relation.c.status == True,
        )
    )
    user_id_to_relation = {r.user: r for r in user_relations}

    # ------------------------------------------------------------
    # 2. Подготовка данных для вставки
    # ------------------------------------------------------------
    cards_to_insert = []  # список словарей для INSERT
    new_contragents_to_insert = []  # список словарей для новых контрагентов
    inserted_contragent_ids = set()

    for item in payload:
        # Обработка организации
        org_id = item.get("organization_id")
        if org_id and org_id in org_by_id:
            organization_id = org_id
        else:
            if not default_org:
                raise HTTPException(400, "Не найдена организация по умолчанию")
            organization_id = default_org.id

        # Обработка контрагента
        contr_id = item.get("contragent_id")
        phone = item.get("phone_number")
        contr_name = item.get("contragent_name", "")

        contragent = None
        if contr_id and contr_id in contr_by_id:
            contragent = contr_by_id[contr_id]
        elif phone and phone in contr_by_phone:
            contragent = contr_by_phone[phone]
        else:
            # Новый контрагент
            phone_code = None
            is_phone_formatted = False
            if phone:
                try:
                    parsed = phonenumbers.parse(str(phone), "RU")
                    if phonenumbers.is_valid_number(parsed):
                        phone_code = geocoder.description_for_number(
                            parsed, "en"
                        ) or region_code_for_number(parsed)
                        is_phone_formatted = True
                except Exception:
                    pass
            new_contr = {
                "name": contr_name,
                "external_id": "",
                "phone": phone,
                "phone_code": phone_code,
                "is_phone_formatted": is_phone_formatted,
                "inn": "",
                "description": "",
                "cashbox": cashbox_id,
                "is_deleted": False,
                "created_at": now_ts,
                "updated_at": now_ts,
            }
            # Проверим, не создали ли уже такого в этом батче
            key = (phone, contr_name)
            if key not in inserted_contragent_ids:
                new_contragents_to_insert.append(new_contr)
                inserted_contragent_ids.add(key)
            # Временно используем placeholder; после вставки заменим на реальный id
            contragent = SimpleNamespace(id=None, phone=phone)

        # Определяем card_number
        card_number = _safe_int_phone(item.get("card_number"))
        if not card_number and contragent.phone:
            card_number = _safe_int_phone(contragent.phone)
        if not card_number:
            card_number = randint(0, 9_223_372_036_854_775)

        # Проверяем существование карты
        if card_number in cards_by_number:
            continue  # уже существует
        if contragent.id and contragent.id in cards_by_contragent:
            continue

        # Получаем настройки лояльности
        setting = settings_by_org.get(organization_id, base_setting)
        cashback_percent = setting.cashback_percent if setting else 0
        minimal_checque_amount = setting.minimal_checque_amount if setting else 0
        max_percentage = setting.max_percentage if setting else 0
        max_withdraw_percentage = setting.max_withdraw_percentage if setting else 0
        start_period = setting.start_period if setting else None
        end_period = setting.end_period if setting else None
        tags = item.get("tags") or (setting.tags if setting else None)

        # Обработка created_by_id через тег USERPHONE_
        created_by_id = user.id
        if tags and "USERPHONE_" in tags:
            tag_phone = tags.split("USERPHONE_")[-1].split(",")[0]
            for u in users_rows:
                if fuzz.ratio(tag_phone, u.phone_number) >= 80:
                    rel = user_id_to_relation.get(u.id)
                    if rel:
                        created_by_id = rel.id
                        break

        # Формируем запись карты
        card_values = {
            "card_number": card_number,
            "tags": tags,
            "balance": 0,
            "income": 0,
            "outcome": 0,
            "cashback_percent": cashback_percent,
            "minimal_checque_amount": minimal_checque_amount,
            "max_percentage": max_percentage,
            "max_withdraw_percentage": max_withdraw_percentage,
            "start_period": start_period,
            "end_period": end_period,
            "contragent_id": contragent.id,  # временно может быть None
            "organization_id": organization_id,
            "cashbox_id": cashbox_id,
            "created_by_id": created_by_id,
            "status_card": item.get("status_card", True),
            "is_deleted": False,
            "lifetime": item.get("lifetime"),
            "apple_wallet_advertisement": item.get(
                "apple_wallet_advertisement", "TableCRM"
            ),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        cards_to_insert.append(
            (card_values, contragent)
        )  # сохраняем ссылку на contragent

    # ------------------------------------------------------------
    # 3. Вставка новых контрагентов пакетом
    # ------------------------------------------------------------
    if new_contragents_to_insert:
        # INSERT ... RETURNING id
        insert_stmt = contragents.insert().returning(
            contragents.c.id, contragents.c.phone
        )
        # Для SQLAlchemy execute many
        new_ids = []
        for c in new_contragents_to_insert:
            res = await database.execute(insert_stmt.values(**c))
            new_ids.append(res)
        # Загружаем созданные записи
        created_contrs = await database.fetch_all(
            contragents.select().where(contragents.c.id.in_(new_ids))
        )
        for c in created_contrs:
            contr_by_id[c.id] = c
            if c.phone:
                contr_by_phone[c.phone] = c

    # ------------------------------------------------------------
    # 4. Вставка карт (batch insert)
    # ------------------------------------------------------------
    inserted_card_ids = []
    if cards_to_insert:
        # Подставляем реальные contragent_id
        final_cards = []
        for card_dict, contragent_obj in cards_to_insert:
            if contragent_obj.id is None:
                # Если контрагент был создан, ищем его по phone
                contragent_obj = contr_by_phone.get(contragent_obj.phone)
            if not contragent_obj:
                continue  # такого быть не должно
            card_dict["contragent_id"] = contragent_obj.id
            final_cards.append(card_dict)

        if final_cards:
            # Выполняем batch insert
            insert_card_stmt = loyality_cards.insert().returning(loyality_cards.c.id)
            for card in final_cards:
                try:
                    new_id = await database.execute(insert_card_stmt.values(**card))
                    inserted_card_ids.append(new_id)
                except Exception as e:
                    # Логируем, но продолжаем
                    print(f"Ошибка при вставке карты: {e}")

    if not inserted_card_ids:
        return []

    # ------------------------------------------------------------
    # 5. Получение созданных карт и отправка уведомлений
    # ------------------------------------------------------------
    created_cards = await database.fetch_all(
        select(*LOYALITY_CARDS_COLUMNS).where(
            loyality_cards.c.id.in_(inserted_card_ids)
        )
    )
    result_cards = []
    for row in created_cards:
        card = dict(row)
        card = datetime_to_timestamp(card)
        card = add_status(card)
        card = await contr_org_ids_to_name(card)
        result_cards.append(card)

    await manager.send_message(
        token,
        {"action": "create", "target": "loyality_cards", "result": result_cards},
    )

    # Фоновое обновление Apple Wallet
    apple_wallet_service = WalletPassGeneratorService()
    for card in result_cards:
        asyncio.create_task(_update_wallet_pass_safe(apple_wallet_service, card["id"]))

    return result_cards


async def _update_wallet_pass_safe(service, card_id: int):
    try:
        await service.update_pass(card_id)
    except Exception:
        pass


# ----------------------------------------------------------------------
# PATCH /loyality_cards/{idx}/ – редактирование
# ----------------------------------------------------------------------
@router.patch("/loyality_cards/{idx}/", response_model=schemas.LoyalityCard)
async def edit_loyality_transaction(
    token: str, idx: int, loyality_card: schemas.LoyalityCardEdit
):
    user = await get_user_by_token(token)
    # Проверяем существование и принадлежность
    existing = await database.fetch_one(
        loyality_cards.select().where(
            loyality_cards.c.id == idx,
            loyality_cards.c.cashbox_id == user.cashbox_id,
            loyality_cards.c.is_deleted == False,
        )
    )
    if not existing:
        raise HTTPException(404, "Карта не найдена")

    values = loyality_card.dict(exclude_unset=True)
    if not values:
        return await get_loyality_card_by_id(token, idx)

    # Валидация процентов
    for p_field in ("max_percentage", "max_withdraw_percentage"):
        if p_field in values and values[p_field] > 100:
            values[p_field] = 100

    # Конвертация timestamp в datetime
    for dt_field in ("start_period", "end_period"):
        if dt_field in values and values[dt_field] is not None:
            values[dt_field] = datetime.fromtimestamp(values[dt_field])

    # UPDATE
    await database.execute(
        loyality_cards.update().where(loyality_cards.c.id == idx).values(**values)
    )

    updated_card = await database.fetch_one(
        select(*LOYALITY_CARDS_COLUMNS).where(loyality_cards.c.id == idx)
    )
    card_dict = dict(updated_card)
    card_dict = datetime_to_timestamp(card_dict)
    card_dict = await contr_org_ids_to_name(card_dict)

    await manager.send_message(
        token,
        {"action": "edit", "target": "loyality_cards", "result": card_dict},
    )
    await update_apple_wallet_pass(card_dict["id"])

    return {**card_dict, "data": {"status": "success"}}


# ----------------------------------------------------------------------
# DELETE /loyality_cards/{idx}/ – мягкое удаление
# ----------------------------------------------------------------------
@router.delete("/loyality_cards/{idx}/", response_model=schemas.LoyalityCard)
async def delete_loyality_transaction(token: str, idx: int):
    user = await get_user_by_token(token)
    # Проверяем существование
    existing = await database.fetch_one(
        loyality_cards.select().where(
            loyality_cards.c.id == idx,
            loyality_cards.c.cashbox_id == user.cashbox_id,
            loyality_cards.c.is_deleted == False,
        )
    )
    if not existing:
        raise HTTPException(404, "Карта не найдена")

    await database.execute(
        loyality_cards.update()
        .where(loyality_cards.c.id == idx)
        .values(is_deleted=True, updated_at=datetime.now())
    )

    deleted_card = await database.fetch_one(
        select(*LOYALITY_CARDS_COLUMNS).where(loyality_cards.c.id == idx)
    )
    card_dict = dict(deleted_card)
    card_dict = datetime_to_timestamp(card_dict)
    card_dict = await contr_org_ids_to_name(card_dict)

    await manager.send_message(
        token,
        {"action": "delete", "target": "loyality_cards", "result": card_dict},
    )
    return {**card_dict, "data": {"status": "success"}}
