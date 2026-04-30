import logging
from datetime import datetime, timedelta, timezone

from database.db import (
    OrderStatus,
    categories,
    docs_sales,
    docs_sales_delivery_info,
    docs_sales_goods,
    docs_sales_tags,
    entity_to_entity,
    global_categories,
    loyality_cards,
    loyality_transactions,
    nomenclature,
    payments,
    pictures,
    tags,
)
from segments.ranges import apply_date_range, apply_range
from sqlalchemy import and_, exists, func, or_, select, text
from sqlalchemy.sql import Select

logger = logging.getLogger(__name__)


def orders_filters(query: Select, order_filters: dict, sub) -> Select:
    where_clauses = []
    if u_at := order_filters.get("updated_at"):
        apply_date_range(sub.c.updated_at, u_at, where_clauses)
    if c_at := order_filters.get("created_at"):
        apply_date_range(sub.c.created_at, c_at, where_clauses)

    if status := order_filters.get("order_status"):
        where_clauses.append(sub.c.order_status == status)

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query


def add_picker_filters(query: Select, picker_filters, sub):
    where_clauses = []

    assigned = picker_filters.get("assigned")

    photos_not_added_minutes = picker_filters.get("photos_not_added_minutes")

    if photos_not_added_minutes is not None and isinstance(
        photos_not_added_minutes, int
    ):
        sales_entity = "docs_sales"

        pictures_of_sales = pictures.c.entity == sales_entity
        pictures_of_order = pictures.c.entity_id == sub.c.id

        # year,month,week,days,hour,min,sec
        interval = func.make_interval(0, 0, 0, 0, 0, photos_not_added_minutes, 0)

        picker_finished = sub.c.picker_finished_at.isnot(None)

        not_any_photo = ~exists(
            select(1).where(and_(pictures_of_sales, pictures_of_order))
        )

        picker_late_photos = func.now() >= sub.c.picker_finished_at + interval

        photo_filter = and_(picker_finished, not_any_photo, picker_late_photos)

        where_clauses.append(photo_filter)

    if assigned is not None:
        apply_range(sub.c.assigned_picker, {"is_none": not assigned}, where_clauses)

    if sdr := picker_filters.get("start"):
        apply_date_range(sub.c.picker_started_at, sdr, where_clauses)
    if fdr := picker_filters.get("finish"):
        apply_date_range(sub.c.picker_finished_at, fdr, where_clauses)

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query


def add_courier_filters(query: Select, courier_filters, sub):
    where_clauses = []

    assigned = courier_filters.get("assigned")

    if assigned is not None:
        apply_range(sub.c.assigned_courier, {"is_none": not assigned}, where_clauses)

        if assigned is False:
            apply_range(
                sub.c.order_status, {"eq": OrderStatus.collected.value}, where_clauses
            )

    if sdr := courier_filters.get("start"):
        apply_date_range(sub.c.courier_picked_at, sdr, where_clauses)
    if fdr := courier_filters.get("finish"):
        apply_date_range(sub.c.courier_delivered_at, fdr, where_clauses)

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query


def add_delivery_required_filters(query: Select, delivery_required: bool, sub):
    if delivery_required is True:
        query = query.where(
            and_(
                docs_sales_delivery_info.c.docs_sales_id.isnot(None),
                docs_sales_delivery_info.c.address.isnot(None),
                docs_sales_delivery_info.c.address != "",
            )
        )
    elif delivery_required is False:
        query = query.where(
            or_(
                docs_sales_delivery_info.c.docs_sales_id.is_(None),
                docs_sales_delivery_info.c.address.is_(None),
                docs_sales_delivery_info.c.address == "",
            )
        )

    return query


def add_purchase_filters(query: Select, purchase_criteria: dict, sub) -> Select:
    """
    Добавляет в запрос фильтры и агрегаты, описанные в purchase_criteria.

    ОПТИМИЗАЦИЯ: используются подзапросы с предварительной фильтрацией
    документов продаж перед применением агрегатов.

    :param query: исходный Select (на sub)
    :param purchase_criteria: словарь из PurchaseCriteria
    :param sub: подзапрос с docs_sales.id, docs_sales.contragent
    :return: модифицированный Select
    """
    where_clauses = []  # фильтры по чекам
    having_clauses = []  # агрегаты по чекам

    # ---- 1. Диапазон дат по чеку ---------------------------------
    if dr := purchase_criteria.get("date_range"):
        apply_date_range(sub.c.created_at, dr, where_clauses)

    # ---- 2. Сумма одного чека -----------------------------------
    if per_check := purchase_criteria.get("amount_per_check"):
        apply_range(sub.c.sum, per_check, where_clauses)

    # ---- 3. Фильтр по категориям / номенклатурам через EXISTS ----
    # ОПТИМИЗАЦИЯ: объединяем условия для категорий в один OR вместо множества EXISTS
    if cats := purchase_criteria.get("categories"):
        # Создаем единственный EXISTS с OR условиями для всех категорий
        category_conditions = [categories.c.name.ilike(f"%{cat}%") for cat in cats]
        exists_clause = (
            select(1)
            .select_from(docs_sales_goods)
            .join(nomenclature, docs_sales_goods.c.nomenclature == nomenclature.c.id)
            .join(categories, nomenclature.c.category == categories.c.id)
            .where(docs_sales_goods.c.docs_sales_id == sub.c.id)
            .where(or_(*category_conditions))
        )
        where_clauses.append(exists(exists_clause))

    # ОПТИМИЗАЦИЯ: аналогично для номенклатур
    if noms := purchase_criteria.get("nomenclatures"):
        nomenclature_conditions = [
            nomenclature.c.name.ilike(f"%{nom}%") for nom in noms
        ]
        exists_clause = (
            select(1)
            .select_from(docs_sales_goods)
            .join(nomenclature, docs_sales_goods.c.nomenclature == nomenclature.c.id)
            .where(docs_sales_goods.c.docs_sales_id == sub.c.id)
            .where(or_(*nomenclature_conditions))
        )
        where_clauses.append(exists(exists_clause))

    if cog := purchase_criteria.get("count_of_goods"):
        query = query.outerjoin(
            docs_sales_goods, docs_sales_goods.c.docs_sales_id == sub.c.id
        )

        apply_range(
            func.coalesce(func.sum(docs_sales_goods.c.quantity), 0), cog, having_clauses
        )

    if purchase_criteria.get("is_fully_paid"):
        # --- сумма оплат в рублях ---
        rub_cte = (
            select(
                entity_to_entity.c.from_id.label("docs_sales_id"),
                func.coalesce(func.sum(payments.c.amount), 0).label("paid_rub"),
            )
            .select_from(entity_to_entity)
            .join(
                payments,
                and_(
                    payments.c.id == entity_to_entity.c.to_id,
                    entity_to_entity.c.to_entity == 5,  # 5 = оплата в рублях
                ),
            )
            .where(entity_to_entity.c.from_entity == 7)  # 7 = документы продаж
            .group_by(entity_to_entity.c.from_id)
            .cte("rub_cte")
        )

        # --- сумма оплат в бонусах ---
        bonus_cte = (
            select(
                entity_to_entity.c.from_id.label("docs_sales_id"),
                func.coalesce(func.sum(loyality_transactions.c.amount), 0).label(
                    "paid_bonus"
                ),
            )
            .select_from(entity_to_entity)
            .join(
                loyality_transactions,
                and_(
                    loyality_transactions.c.id == entity_to_entity.c.to_id,
                    entity_to_entity.c.to_entity == 6,  # 6 = бонусы
                ),
            )
            .where(entity_to_entity.c.from_entity == 7)  # 7 = продажи
            .group_by(entity_to_entity.c.from_id)
            .cte("bonus_cte")
        )
        query = query.outerjoin(rub_cte, rub_cte.c.docs_sales_id == sub.c.id).outerjoin(
            bonus_cte, bonus_cte.c.docs_sales_id == sub.c.id
        )

        # фильтр на полностью оплаченные
        having_clauses.append(
            (
                func.coalesce(func.max(rub_cte.c.paid_rub), 0)
                + func.coalesce(func.max(bonus_cte.c.paid_bonus), 0)
            )
            >= sub.c.sum
        )

    # ---- 4. Агрегаты по контрагенту (COUNT / SUM) -----------------
    # ОПТИМИЗАЦИЯ: применяем WHERE фильтры до агрегации
    if (
        having_clauses
        or purchase_criteria.get("count")
        or purchase_criteria.get("total_amount")
    ):
        # Создаем алиас для docs_sales внутри агрегационного запроса
        docs_sales_agg = docs_sales.alias("docs_sales_agg")

        agg_query = (
            select(
                docs_sales_agg.c.contragent,
                func.count(docs_sales_agg.c.id).label("purchase_count"),
                func.sum(docs_sales_agg.c.sum).label("total_amount"),
            )
            .select_from(docs_sales_agg)
            .where(docs_sales_agg.c.is_deleted == False)
        )

        # Фильтрация по категориям/номенклатурам ДО агрегации
        category_filter_clauses = []

        if cats := purchase_criteria.get("categories"):
            category_conditions = [categories.c.name.ilike(f"%{cat}%") for cat in cats]
            category_exists = exists(
                select(1)
                .select_from(docs_sales_goods)
                .join(
                    nomenclature, docs_sales_goods.c.nomenclature == nomenclature.c.id
                )
                .join(categories, nomenclature.c.category == categories.c.id)
                .where(docs_sales_goods.c.docs_sales_id == docs_sales_agg.c.id)
                .where(or_(*category_conditions))
            )
            category_filter_clauses.append(category_exists)

        if noms := purchase_criteria.get("nomenclatures"):
            nomenclature_conditions = [
                nomenclature.c.name.ilike(f"%{nom}%") for nom in noms
            ]
            nomenclature_exists = exists(
                select(1)
                .select_from(docs_sales_goods)
                .join(
                    nomenclature, docs_sales_goods.c.nomenclature == nomenclature.c.id
                )
                .where(docs_sales_goods.c.docs_sales_id == docs_sales_agg.c.id)
                .where(or_(*nomenclature_conditions))
            )
            category_filter_clauses.append(nomenclature_exists)

        if category_filter_clauses:
            agg_query = agg_query.where(and_(*category_filter_clauses))

        if where_clauses:
            agg_query = agg_query.where(and_(*where_clauses))

        agg_query = agg_query.group_by(docs_sales_agg.c.contragent)

        # HAVING фильтры
        having_clauses_built = []
        if rng := purchase_criteria.get("count"):
            if "gte" in rng:
                having_clauses_built.append(
                    func.count(docs_sales_agg.c.id) >= rng["gte"]
                )
            if "lte" in rng:
                having_clauses_built.append(
                    func.count(docs_sales_agg.c.id) <= rng["lte"]
                )
            if "eq" in rng:
                having_clauses_built.append(
                    func.count(docs_sales_agg.c.id) == rng["eq"]
                )
        if rng := purchase_criteria.get("total_amount"):
            if "gte" in rng:
                having_clauses_built.append(
                    func.sum(docs_sales_agg.c.sum) >= rng["gte"]
                )
            if "lte" in rng:
                having_clauses_built.append(
                    func.sum(docs_sales_agg.c.sum) <= rng["lte"]
                )
            if "eq" in rng:
                having_clauses_built.append(func.sum(docs_sales_agg.c.sum) == rng["eq"])

        if having_clauses_built:
            agg_query = agg_query.having(and_(*having_clauses_built))

        agg_subquery = agg_query.subquery("purchase_aggregates")
        query = query.join(agg_subquery, sub.c.contragent == agg_subquery.c.contragent)
        where_clauses = []  # уже применены

    # ---- 5. Последняя покупка N дней назад -----------------------
    if rng := purchase_criteria.get("last_purchase_days_ago"):
        max_date = func.max(docs_sales.c.created_at)
        sub_last = (
            select(docs_sales.c.contragent, max_date.label("last_purchase"))
            .group_by(docs_sales.c.contragent)
            .subquery()
        )
        query = query.join(sub_last, sub_last.c.contragent == sub.c.contragent)
        if "gte" in rng:
            cutoff = datetime.now(timezone.utc) - timedelta(days=rng["gte"])
            query = query.where(sub_last.c.last_purchase <= cutoff)
        if "lte" in rng:
            cutoff = datetime.now(timezone.utc) - timedelta(days=rng["lte"])
            query = query.where(sub_last.c.last_purchase >= cutoff)

    # ---- 6. Применяем оставшиеся фильтры -------------------------
    if where_clauses:
        query = query.where(and_(*where_clauses))
    if having_clauses:
        query = query.having(and_(*having_clauses))

    # гарантируем уникальные чеки
    query = query.group_by(sub.c.id, sub.c.contragent, sub.c.sum, sub.c.created_at)
    return query


def add_loyality_filters(query: Select, loyality_criteria: dict, sub) -> Select:
    """Фильтры по лояльности.

    Оптимизации:
    - Предварительно фильтруем карты лояльности (balance) в подзапросе.
    - Для expires_in_days используем только последнюю транзакцию (MAX(created_at)).
    """
    where_clauses = []

    if not loyality_criteria:
        return query

    # Подзапрос по картам (возможно с фильтром баланса)
    loyalty_subq = select(
        loyality_cards.c.id.label("card_id"),
        loyality_cards.c.contragent_id,
        loyality_cards.c.balance,
        loyality_cards.c.lifetime,
    ).where(loyality_cards.c.contragent_id.isnot(None))

    if balance := loyality_criteria.get("balance"):
        if "gte" in balance:
            loyalty_subq = loyalty_subq.where(
                loyality_cards.c.balance >= balance["gte"]
            )
        if "lte" in balance:
            loyalty_subq = loyalty_subq.where(
                loyality_cards.c.balance <= balance["lte"]
            )
        if "eq" in balance:
            loyalty_subq = loyalty_subq.where(loyality_cards.c.balance == balance["eq"])

    loyalty_subq = loyalty_subq.subquery("filtered_loyalty_cards")

    # INNER JOIN: нам нужны только контрагенты с подходящими картами
    query = query.join(
        loyalty_subq, loyalty_subq.c.contragent_id == sub.c.contragent, isouter=False
    )

    # Фильтр по сроку действия (дни до истечения)
    if expire := loyality_criteria.get("expires_in_days"):
        # Последняя транзакция на карту
        last_tx = (
            select(
                loyality_transactions.c.loyality_card_id,
                func.max(loyality_transactions.c.created_at).label("last_tx"),
            )
            .group_by(loyality_transactions.c.loyality_card_id)
            .subquery()
        )
        query = query.join(
            last_tx, last_tx.c.loyality_card_id == loyalty_subq.c.card_id
        )
        expiry_datetime = (
            last_tx.c.last_tx + text("INTERVAL '1 second'") * loyalty_subq.c.lifetime
        )
        days_left = func.DATE_PART("day", expiry_datetime - func.now())
        apply_range(days_left, expire, where_clauses)

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query


def created_at_filters(query: Select, data: dict, sub) -> Select:
    where_clauses = []
    apply_date_range(sub.c.created_at, data, where_clauses)
    if where_clauses:
        query = query.where(and_(*where_clauses))
    return query


def tags_filters(query: Select, data: list, sub) -> Select:
    like_conditions = [tags.c.name.ilike(f"%{tag}%") for tag in data]
    return query.where(or_(*like_conditions))


def docs_sales_tags_filters(query: Select, data: list, sub) -> Select:
    like_conditions = [docs_sales_tags.c.name.ilike(f"%{tag}%") for tag in data]
    return query.where(or_(*like_conditions))


def delivery_info_filters(query: Select, data: dict, sub) -> Select:
    where_clauses = []

    if dd := data.get("delivery_date"):
        apply_date_range(docs_sales_delivery_info.c.delivery_date, dd, where_clauses)

    if address := data.get("address"):
        where_clauses.append(docs_sales_delivery_info.c.address.ilike(f"%{address}%"))

    if note := data.get("note"):
        where_clauses.append(docs_sales_delivery_info.c.note.ilike(f"%{note}%"))

    if recipient := data.get("recipient"):
        if not isinstance(recipient, dict):
            recipient = {}
        for k, v in recipient.items():
            where_clauses.append(
                docs_sales_delivery_info.c.recipient.op("->>")(text(f"'{k}'")).ilike(
                    f"%{v}%"
                )
            )

    if where_clauses:
        query = query.where(and_(*where_clauses))

    return query


def add_nomenclatures_filters(query: Select, nomenclature_criteria: dict, sub):
    conditions = list()

    if name := nomenclature_criteria.get("name"):
        conditions.append(nomenclature.c.name.ilike(f"%{name}%"))

    if cats := nomenclature_criteria.get("categories"):
        cat_subquery = select(categories.c.id).where(
            or_(*[categories.c.name.ilike(f"%{cat}%") for cat in cats])
        )
        conditions.append(nomenclature.c.category.in_(cat_subquery))

    if global_cats := nomenclature_criteria.get("global_categories"):
        global_cat_subquery = select(global_categories.c.id).where(
            or_(
                *[
                    global_categories.c.name.ilike(f"%{global_cat}%")
                    for global_cat in global_cats
                ]
            )
        )
        conditions.append(nomenclature.c.global_category_id.in_(global_cat_subquery))

    if (has_photo := nomenclature_criteria.get("has_photo")) is not None:
        picture_subquery = select(pictures.c.entity_id).where(
            pictures.c.entity == "nomenclature",
            pictures.c.entity_id == nomenclature.c.id,
            pictures.c.is_deleted.isnot(True),
        )
        condition = (
            nomenclature.c.id.in_(picture_subquery)
            if has_photo
            else ~nomenclature.c.id.in_(picture_subquery)
        )
        conditions.append(condition)

    if tags := nomenclature_criteria.get("tags"):
        [conditions.append(nomenclature.c.tags.any(tag)) for tag in tags]

    if conditions:
        query = query.where(or_(*conditions))

    return query
