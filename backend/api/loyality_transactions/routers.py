import asyncio
from datetime import datetime

import api.loyality_transactions.schemas as schemas
from database.db import (
    async_session_maker,
    database,
    loyality_cards,
    loyality_transactions,
)
from fastapi import APIRouter, Depends
from functions.helpers import (
    datetime_to_timestamp,
    get_entity_by_id_and_created_by,
    get_entity_by_id_cashbox,
    get_filters_transactions,
    get_user_by_token,
)
from sqlalchemy import Numeric, case, cast, desc, func, literal, select, update
from ws_manager import manager

router = APIRouter(tags=["loyality_transactions"])


# Асинхронная функция подсчёта баланса бонусов по ID карты
async def raschet_bonuses(card_id: int) -> None:
    # Открываем асинхронную сессию SQLAlchemy
    async with async_session_maker() as session:
        # -----------------------------------------------
        # Шаг 1. Формируем подзапрос для доходов (accrual)
        # select SUM(amount) where type='accrual'
        a_q = func.coalesce(
            func.sum(
                case(
                    (
                        loyality_transactions.c.type == "accrual",
                        loyality_transactions.c.amount,
                    )
                )
            ),
            0,
        ).label("income")

        # -----------------------------------------------
        # Шаг 2. Формируем подзапрос для расходов (withdraw)
        # select SUM(amount) where type='withdraw'
        w_q = func.coalesce(
            func.sum(
                case(
                    (
                        loyality_transactions.c.type == "withdraw",
                        loyality_transactions.c.amount,
                    )
                )
            ),
            0,
        ).label("outcome")

        # -----------------------------------------------
        # Шаг 3. Основной запрос для вычисления баланса
        # ROUND(CASE WHEN income > outcome THEN income - outcome ELSE 0 END, 2)
        balance_q = func.round(
            cast(case((a_q > w_q, a_q - w_q), else_=0), Numeric), literal(2)
        ).label("balance")

        # -----------------------------------------------
        # Шаг 4. Объединяем всё в SELECT и фильтруем по условиям
        query = select(a_q, w_q, balance_q).where(
            loyality_transactions.c.loyality_card_id == card_id,
            loyality_transactions.c.status.is_(True),
            loyality_transactions.c.is_deleted.is_(False),
        )

        # Выполняем запрос и получаем объект результата
        result = await session.execute(query)
        res = result.first()
        if not res:
            # Если нет транзакций — выходим
            return

        # Преобразуем результат в словарь
        edit_dict = dict(res._mapping)

        # -----------------------------------------------
        # Шаг 5. Обновляем таблицу loyality_cards на основании расчёта
        update_stmt = (
            update(loyality_cards)
            .where(loyality_cards.c.id == card_id)
            .values(
                income=edit_dict["income"],
                outcome=edit_dict["outcome"],
                balance=edit_dict["balance"],
            )
        )

        # Выполняем UPDATE и фиксируем изменения
        await session.execute(update_stmt)
        await session.commit()


@router.get("/loyality_transactions/{idx}/", response_model=schemas.LoyalityTransaction)
async def get_loyality_transaction_by_id(token: str, idx: int):
    """Получение транзакции по ID"""
    user = await get_user_by_token(token)
    loyality_transactions_db = await get_entity_by_id_and_created_by(
        loyality_transactions, idx, user.id
    )
    loyality_transactions_db = datetime_to_timestamp(loyality_transactions_db)

    return loyality_transactions_db


@router.get("/loyality_transactions/", response_model=schemas.CountRes)
async def get_transactions(
    token: str,
    limit: int = 100,
    offset: int = 0,
    filters_q: schemas.LoyalityTranstactionFilters = Depends(),
    sort: str = "id:desc",
):
    """Получение списка транзакций"""
    user = await get_user_by_token(token)
    filters = get_filters_transactions(loyality_transactions, filters_q)

    order_column = desc(loyality_transactions.c.id)
    if sort:
        sort_field, _, sort_direction = sort.partition(":")
        column = getattr(loyality_transactions.c, sort_field, None)
        if column is not None:
            sort_direction = sort_direction.lower() or "asc"
            order_column = column.desc() if sort_direction == "desc" else column.asc()

    query = (
        loyality_transactions.select()
        .where(
            loyality_transactions.c.cashbox == user.cashbox_id,
            loyality_transactions.c.is_deleted.is_not(True),
        )
        .order_by(order_column)
    )
    count_query = select(func.count(loyality_transactions.c.id)).where(
        loyality_transactions.c.cashbox == user.cashbox_id,
        loyality_transactions.c.is_deleted.is_not(True),
    )

    if filters:
        query = query.filter(*filters)
        count_query = count_query.filter(*filters)

    query = query.limit(limit).offset(offset)
    loyality_transactions_db = await database.fetch_all(query)
    loyality_transactions_db = [*map(datetime_to_timestamp, loyality_transactions_db)]

    count = await database.execute(count_query)

    return {"result": loyality_transactions_db, "count": count}


@router.patch(
    "/loyality_transactions/{idx}/", response_model=schemas.LoyalityTransaction
)
async def edit_loyality_transaction(
    token: str,
    idx: int,
    loyality_transaction: schemas.LoyalityTransactionEdit,
):
    """Редактирование транзакций"""
    user = await get_user_by_token(token)
    loyality_transaction_db = await get_entity_by_id_cashbox(
        loyality_transactions, idx, user.cashbox_id
    )
    loyality_transaction_values = loyality_transaction.dict(exclude_unset=True)

    if loyality_transaction_values:

        if loyality_transaction_values.get("dated"):
            loyality_transaction_values["dated"] = datetime.fromtimestamp(
                loyality_transaction_values["dated"]
            )

        if loyality_transaction_values.get("loyality_card_id"):
            if (
                loyality_transaction_values["loyality_card_id"]
                != loyality_transaction_db.loyality_card_id
            ):
                new_card = await database.fetch_one(
                    loyality_cards.select().where(
                        loyality_cards.c.id
                        == loyality_transaction_values["loyality_card_id"],
                        loyality_cards.c.cashbox_id == user.cashbox_id,
                    )
                )
                loyality_transaction_values["loyality_card_number"] = (
                    new_card.card_number
                )

        query = (
            loyality_transactions.update()
            .where(
                loyality_transactions.c.id == idx,
                loyality_transactions.c.cashbox == user.cashbox_id,
            )
            .values(loyality_transaction_values)
        )
        await database.execute(query)
        loyality_transaction_db = await get_entity_by_id_cashbox(
            loyality_transactions, idx, user.cashbox_id
        )

    loyality_transaction_db = datetime_to_timestamp(loyality_transaction_db)

    await manager.send_message(
        token,
        {
            "action": "edit",
            "target": "loyality_transactions",
            "result": loyality_transaction_db,
        },
    )

    card_idx = (
        select(loyality_transactions.c.loyality_card_id)
        .where(loyality_transactions.c.id == idx)
        .limit(1)
    )
    card_idx = await database.fetch_val(card_idx, column=0)

    await asyncio.gather(asyncio.create_task(raschet_bonuses(card_idx)))

    return {**loyality_transaction_db, **{"data": {"status": "success"}}}


@router.delete(
    "/loyality_transactions/{idx}/", response_model=schemas.LoyalityTransaction
)
async def delete_loyality_transaction(token: str, idx: int):
    """Удаление транзакций"""
    user = await get_user_by_token(token)

    await get_entity_by_id_cashbox(loyality_transactions, idx, user.cashbox_id)

    query = (
        loyality_transactions.update()
        .where(
            loyality_transactions.c.id == idx,
            loyality_transactions.c.cashbox == user.cashbox_id,
        )
        .values({"is_deleted": True})
    )
    await database.execute(query)

    query = loyality_transactions.select().where(
        loyality_transactions.c.id == idx,
        loyality_transactions.c.cashbox == user.cashbox_id,
    )
    loyality_transaction_db = await database.fetch_one(query)

    loyality_card_id = loyality_transaction_db.loyality_card_id

    loyality_transaction_db = datetime_to_timestamp(loyality_transaction_db)

    await manager.send_message(
        token,
        {
            "action": "delete",
            "target": "loyality_transactions",
            "result": loyality_transaction_db,
        },
    )

    await asyncio.gather(asyncio.create_task(raschet_bonuses(loyality_card_id)))

    return {**loyality_transaction_db, **{"data": {"status": "success"}}}
