import uuid
from datetime import datetime, timedelta
from typing import Optional

import api.analytics.schemas as analytics_schemas
import functions.filter_schemas as filter_schemas
from const import PaymentType
from database.db import (
    contragents,
    database,
    loyality_cards,
    users,
    users_cboxes_relation,
)
from fastapi import APIRouter, Depends, HTTPException
from functions.helpers import get_filters_analytics
from sqlalchemy import func, select

router = APIRouter(tags=["analytics"])


@router.get("/analytics/", response_model=analytics_schemas.PaymentsAnalytics)
async def analytics(
    token: str,
    entity: str = "payments",
    filter_schema: filter_schemas.AnalyticsFiltersQuery = Depends(),
    offset: int = 0,
    limit: int = 100,
    sort: str = "percentage:desc",
    type: str = f"{PaymentType.incoming}, {PaymentType.outgoing}",
):
    """Аналитика платежей"""
    query_user = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query_user)
    if user:
        if user.status:
            filters = get_filters_analytics(filter_schema)
            payment_directions = type.split(", ")
            cashbox = user.cashbox_id
            queries = []
            for payment_direction in payment_directions:
                query_sums = (
                    "("
                    + f"""
                    SELECT articles.name AS name, payments.type AS type, sum(payments.amount) AS sum
                    FROM payments JOIN articles ON articles.id = payments.article_id
                    WHERE payments.cashbox = {cashbox} AND payments.type = '{payment_direction}' AND payments.is_deleted != true
                    """
                    + filters
                    + " GROUP BY articles.name, payments.type"
                    + ") as sums"
                )
                query_articles = (
                    "("
                    + f"""
                    SELECT articles.id AS id, articles.name AS name, articles.emoji AS emoji, articles.icon_file AS icon_file
                    FROM articles
                    WHERE articles.cashbox = {cashbox}
                """
                    + ") as articles"
                )
                total_column = (
                    f"""
                    SELECT sum(payments.amount) AS total
                    FROM payments JOIN articles ON articles.id = payments.article_id
                    WHERE payments.cashbox = {cashbox} AND payments.type = '{payment_direction}' AND payments.is_deleted != true
                """
                    + filters
                )
                queries_joined = (
                    query_articles
                    + " JOIN "
                    + query_sums
                    + " ON sums.name=articles.name"
                )

                queries.append(
                    f"""
                    SELECT articles.id, articles.icon_file, articles.emoji, articles.name, sums.type, sums.sum,
                        sums.sum * 100 / ({total_column}) as percentage
                    FROM {queries_joined}
                    """
                )
            query = queries.pop(0)
            for subquery in queries:
                query = query + " UNION " + subquery

            sort_name, sort_direction = sort.split(":")[:2]
            if sort_name not in ("percentage",):
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )
            if sort_direction in ("desc", "asc"):
                query = query + f"ORDER BY {sort_name} {sort_direction}"
            else:
                raise HTTPException(
                    status_code=400, detail="Вы ввели некорректный параметр сортировки!"
                )
            query = query + f" LIMIT {limit} OFFSET {offset}"
            articles_db = await database.fetch_all(query)

            result = map(
                lambda row: analytics_schemas.PaymentAnalytics(
                    article_id=row[0],
                    article_image=row[1],
                    article_emoji=row[2],
                    article_name=row[3],
                    type=row[4],
                    sum=row[5],
                    percentage=row[6],
                ),
                articles_db,
            )
            return [*result]
    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/analytics_cards/")
async def analytics(
    token: str,
    date_from: int,
    date_to: int,
    user_id: Optional[int] = None,
):

    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    """Аналитика карт лояльности"""
    query_user = users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    user = await database.fetch_one(query_user)
    if user:
        if user.status:
            start_date = datetime.fromtimestamp(date_from)
            end_date = datetime.fromtimestamp(date_to)

            res = []

            filters = [users_cboxes_relation.c.cashbox_id == user.cashbox_id]
            if user_id:
                filters.append(users_cboxes_relation.c.user == user_id)

            all_distinct_users_q = users_cboxes_relation.select().where(*filters)
            all_distinct_users = await database.fetch_all(all_distinct_users_q)

            for c_user in all_distinct_users:
                user_tg_q = users.select().where(users.c.id == c_user.user)
                user_tg = await database.fetch_one(user_tg_q)

                q = select(func.count(loyality_cards.c.id)).where(
                    loyality_cards.c.cashbox_id == user.cashbox_id,
                    loyality_cards.c.created_by_id == c_user.id,
                )
                all_cards = await database.fetch_one(q)

                user_body = {
                    "username": user_tg.username,
                    "user_id": user_tg.id,
                    "first_name": user_tg.first_name,
                    "all_count": all_cards.count_1,
                }
                subres = []

                for single_date in daterange(start_date, end_date):
                    day_start = single_date.replace(
                        hour=0, minute=0, second=0, microsecond=000000
                    )
                    day_end = single_date.replace(
                        hour=23, minute=59, second=59, microsecond=999999
                    )

                    q = loyality_cards.select().where(
                        loyality_cards.c.cashbox_id == user.cashbox_id,
                        loyality_cards.c.created_at >= day_start,
                        loyality_cards.c.created_at <= day_end,
                        loyality_cards.c.created_by_id == c_user.id,
                    )
                    all_cards = await database.fetch_all(q)

                    subres_body = {
                        "id": uuid.uuid4(),
                        "date": single_date.strftime("%d-%m-%Y"),
                        "day_count": len(all_cards),
                    }

                    subres.append(subres_body)

                user_body["result"] = subres
                res.append(user_body)
            return res

    raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")


@router.get("/analytics/contragents-duplicates/")
async def contragents_duplicates_analytics(
    token: str,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
    offset: int = 0,
    limit: int = 100,
    sort_asc: bool = True,
):
    user = await database.fetch_one(
        users_cboxes_relation.select(users_cboxes_relation.c.token == token)
    )
    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")

    start_date = datetime.fromtimestamp(date_from) if date_from else None
    end_date = datetime.fromtimestamp(date_to) if date_to else None

    filters_cards = [
        loyality_cards.c.is_deleted.is_(False),
        contragents.c.is_deleted.is_(False),
    ]
    if start_date:
        filters_cards.append(loyality_cards.c.created_at >= start_date)
    if end_date:
        filters_cards.append(loyality_cards.c.created_at <= end_date)

    """
    Ищет дубли по номеру телефона контрагента среди карт лояльности
    """
    query_cards_duplicate = (
        select(
            contragents.c.phone.label("phone"),
            func.count(loyality_cards.c.id).label("cards_count"),
            func.array_agg(loyality_cards.c.created_at).label("cards_created_at"),
        )
        .select_from(
            loyality_cards.join(
                contragents,
                loyality_cards.c.contragent_id == contragents.c.id,
            )
        )
        .where(*filters_cards)
        .group_by(contragents.c.phone)
        .having(func.count(loyality_cards.c.id) > 1)
        .limit(limit)
        .offset(offset)
    )

    cards_duplicate_rows = await database.fetch_all(query_cards_duplicate)
    cards_duplicate_result = []
    for r in cards_duplicate_rows:
        sorted_dates = sorted(
            r.cards_created_at,
            reverse=not sort_asc,
        )

        cards_duplicate_result.append(
            {
                "phone": r.phone,
                "cards_count": r.cards_count,
                "cards_created_at": [dt.isoformat() for dt in sorted_dates],
            }
        )
    total_cards_duplicates = sum([p["cards_count"] for p in cards_duplicate_result])

    """
    Ищет дубли по номеру телефона среди контрагентов
    """
    filters_contragents = [contragents.c.is_deleted.is_(False)]
    if date_from:
        filters_contragents.append(contragents.c.created_at >= date_from)
    if date_to:
        filters_contragents.append(contragents.c.created_at <= date_to)

    query_contragents_duplicate = (
        select(
            contragents.c.phone.label("phone"),
            func.count(func.distinct(contragents.c.id)).label("contragents_count"),
            func.array_agg(func.to_timestamp(contragents.c.created_at)).label(
                "contragents_created_at"
            ),
        )
        .select_from(contragents)
        .where(*filters_contragents)
        .group_by(contragents.c.phone)
        .having(func.count(func.distinct(contragents.c.id)) > 1)
        .limit(limit)
        .offset(offset)
    )
    contragents_duplicate_rows = await database.fetch_all(query_contragents_duplicate)
    contragents_duplicate_result = []
    for r in contragents_duplicate_rows:
        sorted_dates = sorted(
            r.contragents_created_at,
            reverse=not sort_asc,
        )

        contragents_duplicate_result.append(
            {
                "phone": r.phone,
                "contragents_count": r.contragents_count,
                "contragents_created_at": [dt.isoformat() for dt in sorted_dates],
            }
        )
    total_contragents_duplicates = sum(
        [p["contragents_count"] for p in contragents_duplicate_result]
    )

    return {
        "total_cards_duplicates": total_cards_duplicates,
        "loyality_cards_duplicates": cards_duplicate_result,
        "total_contragents_duplicates": total_contragents_duplicates,
        "contragents_duplicates": contragents_duplicate_result,
    }
