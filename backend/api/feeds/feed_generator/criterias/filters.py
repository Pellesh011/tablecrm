from collections import defaultdict

from api.tech_cards.models import TechCardDB, TechCardItemDB
from database.db import (
    categories,
    database,
    nomenclature,
    nomenclature_attributes,
    nomenclature_attributes_value,
    pictures,
    price_types,
    prices,
    users_cboxes_relation,
    warehouse_balances_latest,
)
from sqlalchemy import Integer, and_, cast, func, or_, select


class FeedCriteriaFilter:
    def __init__(self, criteria_data: dict, cashbox_id):
        self.criteria_data = criteria_data
        self.cashbox_id = cashbox_id

    def add_filters(self, query, q, price_column=None):
        """Добавляем фильтры к запросу"""
        criteria = self.criteria_data
        current_price_column = (
            price_column if price_column is not None else prices.c.price
        )

        # warehouse_id фильтр пока не поддерживается без warehouse_register_movement
        # if criteria.get("warehouse_id"):
        #     query = query.where(
        #         warehouse_register_movement.c.warehouse_id.in_(criteria["warehouse_id"])
        #     )

        if criteria.get("category_id"):
            query = query.where(nomenclature.c.category.in_(criteria["category_id"]))

        if criteria.get("prices"):
            if criteria["prices"].get("from"):
                query = query.where(current_price_column >= criteria["prices"]["from"])
            if criteria["prices"].get("to"):
                query = query.where(current_price_column <= criteria["prices"]["to"])

        tags = criteria.get("tags")
        if isinstance(tags, str):
            tags = [item.strip() for item in tags.split(",") if item.strip()]
        elif isinstance(tags, list):
            tags = [
                item.strip() for item in tags if isinstance(item, str) and item.strip()
            ]
        else:
            tags = None
        if tags:
            query = query.where(nomenclature.c.tags.overlap(tags))

        # exclude_nomenclature_ids: исключаем товары по списку ID номенклатур
        exclude_ids = criteria.get("exclude_nomenclature_ids")
        if exclude_ids:
            query = query.where(nomenclature.c.id.notin_(exclude_ids))

        # only_on_stock фильтр пока не поддерживается без warehouse_register_movement
        # if criteria.get("only_on_stock") and q:
        #     query = query.having(func.sum(q) > 0)

        return query

    async def get_warehouse_balance(self):
        import logging

        logger = logging.getLogger(__name__)

        if not self.cashbox_id:
            logger.warning("FeedCriteriaFilter: cashbox_id is None")
            return []

        logger.info(f"FeedCriteriaFilter: cashbox_id={self.cashbox_id}")
        price_type_id = self.criteria_data.get("price_types_id")
        if not price_type_id:
            query = select(price_types).where(price_types.c.cashbox == self.cashbox_id)
            types = await database.fetch_all(query)
            logger.info(
                f"FeedCriteriaFilter: Found {len(types)} price types for cashbox_id={self.cashbox_id}"
            )
            price_type_id = types[0].id if types else None
            logger.info(f"FeedCriteriaFilter: Using price_type_id={price_type_id}")

        if not price_type_id:
            logger.warning(
                f"FeedCriteriaFilter: No price_type_id found for cashbox_id={self.cashbox_id}"
            )
            return []

        # Проверяем, есть ли товары вообще
        check_query = select(func.count(nomenclature.c.id)).where(
            and_(
                nomenclature.c.cashbox == self.cashbox_id,
                nomenclature.c.is_deleted.is_not(True),
            )
        )
        total_nomenclature = await database.fetch_one(check_query)
        logger.debug(
            f"FeedCriteriaFilter: Total nomenclature count for cashbox_id={self.cashbox_id}: {total_nomenclature[0] if total_nomenclature else 0}"
        )

        # Проверяем, есть ли цены
        check_prices_query = select(func.count(prices.c.id)).where(
            and_(
                prices.c.price_type == price_type_id,
                prices.c.cashbox == self.cashbox_id,
                prices.c.is_deleted.is_not(True),
            )
        )
        total_prices = await database.fetch_one(check_prices_query)

        tech_cards = TechCardDB.__table__
        tech_card_items = TechCardItemDB.__table__
        component_nomenclature = nomenclature.alias("component_nomenclature")

        tech_cards_flag = self.criteria_data.get("tech_cards")
        if isinstance(tech_cards_flag, str):
            tech_cards_only = tech_cards_flag.strip().lower() in (
                "1",
                "true",
                "yes",
                "y",
            )
        else:
            tech_cards_only = bool(tech_cards_flag)

        # exclude_component_ids: компоненты с такими ID не участвуют в пересчете техкарт
        exclude_component_ids = self.criteria_data.get("exclude_component_ids")

        # balances_sq: суммарный остаток по каждому компоненту по последним
        # записям остатков на складах. Историю движений здесь не суммируем.
        warehouse_ids = self.criteria_data.get("warehouse_id")
        if isinstance(warehouse_ids, list):
            warehouse_ids = [w for w in warehouse_ids if isinstance(w, int)]
        else:
            warehouse_ids = None
        latest_balances_sq_query = select(
            warehouse_balances_latest.c.organization_id,
            warehouse_balances_latest.c.warehouse_id,
            warehouse_balances_latest.c.nomenclature_id,
            warehouse_balances_latest.c.current_amount,
        ).where(warehouse_balances_latest.c.cashbox_id == self.cashbox_id)
        if warehouse_ids:
            latest_balances_sq_query = latest_balances_sq_query.where(
                warehouse_balances_latest.c.warehouse_id.in_(warehouse_ids)
            )
        latest_balances_sq = latest_balances_sq_query.subquery()
        balances_sq_query = select(
            latest_balances_sq.c.nomenclature_id.label("component_id"),
            func.sum(latest_balances_sq.c.current_amount).label("component_stock"),
        ).group_by(latest_balances_sq.c.nomenclature_id)
        balances_sq = balances_sq_query.subquery()

        # available_sq: доступный выпуск готового продукта по техкарте = min(остаток компонента / расход)
        available_sq_query = (
            select(
                tech_cards.c.parent_nomenclature_id.label("nomenclature_id"),
                func.min(
                    func.coalesce(
                        func.floor(
                            func.coalesce(balances_sq.c.component_stock, 0)
                            / func.nullif(tech_card_items.c.quantity, 0)
                        ),
                        0,
                    )
                ).label("available_units"),
            )
            .select_from(
                tech_cards.join(
                    users_cboxes_relation,
                    tech_cards.c.user_id == users_cboxes_relation.c.id,
                )
                .join(
                    tech_card_items,
                    tech_card_items.c.tech_card_id == tech_cards.c.id,
                )
                .outerjoin(
                    component_nomenclature,
                    component_nomenclature.c.id == tech_card_items.c.nomenclature_id,
                )
                .outerjoin(
                    balances_sq,
                    balances_sq.c.component_id == tech_card_items.c.nomenclature_id,
                )
            )
            .where(
                tech_cards.c.status != "deleted",
                users_cboxes_relation.c.cashbox_id == self.cashbox_id,
                or_(
                    component_nomenclature.c.type.is_(None),
                    component_nomenclature.c.type.notin_(["service", "work"]),
                ),
            )
        )
        if exclude_component_ids:
            available_sq_query = available_sq_query.where(
                tech_card_items.c.nomenclature_id.notin_(exclude_component_ids)
            )
        available_sq = available_sq_query.group_by(
            tech_cards.c.parent_nomenclature_id
        ).subquery()

        # stock_sq: суммарный остаток товара по последним остаткам на складах
        stock_sq_query = select(
            latest_balances_sq.c.nomenclature_id.label("nomenclature_id"),
            func.sum(latest_balances_sq.c.current_amount).label("current_amount"),
        ).group_by(latest_balances_sq.c.nomenclature_id)
        stock_sq = stock_sq_query.subquery()

        # Для фида нужна одна цена на товар.
        # Если записей несколько, берём самую свежую.
        ranked_prices_sq = (
            select(
                prices.c.nomenclature.label("nomenclature_id"),
                prices.c.price.label("price"),
                func.row_number()
                .over(
                    partition_by=prices.c.nomenclature,
                    order_by=(
                        func.coalesce(prices.c.date_from, 0).desc(),
                        prices.c.updated_at.desc(),
                        prices.c.id.desc(),
                    ),
                )
                .label("rn"),
            )
            .where(
                and_(
                    prices.c.price_type == price_type_id,
                    prices.c.cashbox == self.cashbox_id,
                    prices.c.is_deleted.is_not(True),
                )
            )
            .subquery()
        )

        current_prices_sq = (
            select(
                ranked_prices_sq.c.nomenclature_id,
                ranked_prices_sq.c.price,
            )
            .where(ranked_prices_sq.c.rn == 1)
            .subquery()
        )

        query = (
            select(
                nomenclature.c.id.label("id"),
                nomenclature.c.name.label("name"),
                func.coalesce(categories.c.name, "").label("category"),
                nomenclature.c.description_short.label("description"),
                nomenclature.c.description_long.label("description_long"),
                nomenclature.c.seo_title.label("seo_title"),
                nomenclature.c.seo_description.label("seo_description"),
                nomenclature.c.seo_keywords.label("seo_keywords"),
                current_prices_sq.c.price.label("price"),
                cast(None, Integer).label("warehouse_id"),
                (
                    func.coalesce(available_sq.c.available_units, 0)
                    if tech_cards_only
                    else func.coalesce(stock_sq.c.current_amount, 0)
                ).label("current_amount"),
            )
            .select_from(
                nomenclature.join(
                    current_prices_sq,
                    current_prices_sq.c.nomenclature_id == nomenclature.c.id,
                ).outerjoin(  # Используем outerjoin для категорий, чтобы товары без категорий тоже попадали
                    categories,
                    categories.c.id == nomenclature.c.category,
                )
            )
            .where(nomenclature.c.cashbox == self.cashbox_id)
            .where(
                or_(
                    nomenclature.c.is_deleted.is_(None),
                    nomenclature.c.is_deleted.is_(False),
                )
            )
            .distinct()
        )

        if tech_cards_only:
            query = query.join(
                available_sq,
                available_sq.c.nomenclature_id == nomenclature.c.id,
            ).where(available_sq.c.available_units > 0)
        else:
            query = query.outerjoin(
                stock_sq,
                stock_sq.c.nomenclature_id == nomenclature.c.id,
            )

        # применяем фильтры (без q, так как мы не используем warehouse_register_movement)
        query = self.add_filters(query, None, current_prices_sq.c.price)

        # выполняем запрос
        rows = await database.fetch_all(query)
        results = [dict(row) for row in rows]

        import logging

        logger = logging.getLogger(__name__)
        logger.info(
            f"FeedCriteriaFilter: Found {len(results)} products for cashbox_id={self.cashbox_id}"
        )

        if not results:
            return []

        # достаём все id номенклатур
        nomenclature_ids = [r["id"] for r in results]

        # тянем картинки пачкой
        # Берём id, чтобы при необходимости можно было строить стабильные публичные ссылки
        images_query = (
            select(pictures.c.id, pictures.c.entity_id, pictures.c.url)
            .where(
                and_(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id.in_(nomenclature_ids),
                    pictures.c.is_deleted.is_not(True),
                )
            )
            .order_by(
                pictures.c.entity_id, pictures.c.is_main.desc(), pictures.c.id.asc()
            )
        )
        images_rows = await database.fetch_all(images_query)

        # группируем картинки по номенклатуре
        from common.utils.url_helper import get_app_url_for_environment

        app_url = get_app_url_for_environment() or "app.tablecrm.com"  # fallback
        app_url = app_url.rstrip("/")
        if not app_url.startswith(("http://", "https://")):
            app_url = f"https://{app_url}"

        def _normalize_picture_url(raw_url: str) -> str:
            """Нормализует ссылку на картинку для внешнего потребителя (Tilda).

            В БД `pictures.url` обычно лежит S3 key вида:
            - photos/2025/12/21/<cashbox>/<uuid>.jpg
            Но в старых данных может быть просто имя файла.

            Для Tilda важно отдавать публичный URL без авторизации, поэтому
            приводим к виду: {APP_URL}/api/v1/photos/<path>
            """

            url = (raw_url or "").strip()
            if not url:
                return ""

            # Если уже абсолютный URL — оставляем как есть
            if url.startswith(("http://", "https://")):
                return url

            # Иногда в БД может лежать уже API-путь
            url = url.lstrip("/")

            # Если это странный "прокси"-вид /api/v1/https://..., раскручиваем до https://...
            if url.startswith("api/v1/https://"):
                return url.split("api/v1/", 1)[1]
            if url.startswith("api/v1/http://"):
                return url.split("api/v1/", 1)[1]

            # Приводим к /photos/<path> (эндпоинт публичный)
            if url.startswith("api/v1/photos/"):
                url = url.split("api/v1/photos/", 1)[1]
            elif url.startswith("photos/"):
                url = url.split("photos/", 1)[1]

            return f"{app_url}/api/v1/photos-tilda/{url}"

        images_map = defaultdict(list)
        for row in images_rows:
            normalized = _normalize_picture_url(row.url)
            if normalized and normalized not in images_map[row.entity_id]:
                images_map[row.entity_id].append(normalized)

        # ---- атрибуты ----
        attrs_query = (
            select(
                nomenclature_attributes_value.c.nomenclature_id,
                nomenclature_attributes.c.name,
                nomenclature_attributes_value.c.value,
            )
            .join(
                nomenclature_attributes,
                nomenclature_attributes.c.id
                == nomenclature_attributes_value.c.attribute_id,
            )
            .where(
                and_(
                    nomenclature_attributes_value.c.nomenclature_id.in_(
                        nomenclature_ids
                    ),
                    nomenclature_attributes.c.cashbox == self.cashbox_id,
                )
            )
        )
        attrs_rows = await database.fetch_all(attrs_query)
        attrs_map = defaultdict(dict)
        for row in attrs_rows:
            attrs_map[row.nomenclature_id][row.name] = row.value

        # ---- собираем финальный результат ----
        for r in results:
            # images должен быть списком, а не None
            r["images"] = images_map.get(r["id"], [])
            # params должен быть словарем, а не None
            r["params"] = attrs_map.get(r["id"], {})

        return results
