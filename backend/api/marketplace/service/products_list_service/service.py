import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime
from typing import List, Optional

from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.products_list_service.schemas import (
    AvailableWarehouse,
    MarketplaceProduct,
    MarketplaceProductAttribute,
    MarketplaceProductDetail,
    MarketplaceProductList,
    MarketplaceProductsRequest,
    MarketplaceProductUserAdmin,
    MarketplaceSort,
    product_buttons_text,
)
from api.marketplace.service.public_categories.public_categories_service import (
    MarketplacePublicCategoriesService,
)
from api.nomenclature.video.models import nomenclature_videos
from common.geocoders.instance import geocoder
from common.utils.url_helper import get_app_url_for_environment
from database.db import (
    categories,
    cboxes,
    database,
    docs_sales_goods,
    manufacturers,
    marketplace_rating_aggregates,
    marketplace_searches,
    nomenclature,
    nomenclature_attributes,
    nomenclature_attributes_value,
    nomenclature_barcodes,
    nomenclature_groups,
    nomenclature_groups_value,
    pictures,
    price_types,
    prices,
    units,
    users,
    warehouse_balances_latest,
    warehouses,
)
from fastapi import HTTPException
from sqlalchemy import (
    Float,
    Integer,
    and_,
    asc,
    case,
    cast,
    desc,
    func,
    literal_column,
    or_,
    select,
    union_all,
)


class MarketplaceProductsListService(BaseMarketplaceService):
    @staticmethod
    def __transform_photo_route(photo_path: Optional[str]) -> Optional[str]:
        if not photo_path:
            return None
        base_url = get_app_url_for_environment()
        if not base_url:
            raise ValueError("APP_URL не настроен для текущего окружения")
        photo_url = photo_path.lstrip("/")

        if "seller" in photo_url:
            return f"https://{base_url}/api/v1/{photo_path.lstrip('/')}"
        else:
            return f"https://{base_url}/{photo_path.lstrip('/')}"

    @staticmethod
    def __build_picture_url(picture_id: Optional[int]) -> Optional[str]:
        """Строит публичный URL для фото по ID"""
        if not picture_id:
            return None
        base_url = get_app_url_for_environment()
        if not base_url:
            return None
        # Добавляем протокол, если его нет
        if not base_url.startswith(("http://", "https://")):
            base_url = f"https://{base_url}"
        return f"{base_url}/api/v1/pictures/{picture_id}/content"

    @staticmethod
    def _haversine_sql(client_lat: float, client_lon: float, lat_col, lon_col):
        """
        Возвращает SQLAlchemy-выражение для расстояния в км по формуле Хаверсина.
        Если координаты NULL — возвращает 999999.0 (сортируется последним).
        """
        client_lat_rad = func.radians(literal_column(str(client_lat)))
        client_lon_rad = func.radians(literal_column(str(client_lon)))
        lat_rad = func.radians(func.cast(lat_col, Float))
        lon_rad = func.radians(func.cast(lon_col, Float))
        dlat = lat_rad - client_lat_rad
        dlon = lon_rad - client_lon_rad
        a = func.pow(
            func.sin(dlat / literal_column("2.0")), literal_column("2.0")
        ) + func.cos(client_lat_rad) * func.cos(lat_rad) * func.pow(
            func.sin(dlon / literal_column("2.0")), literal_column("2.0")
        )
        a_safe = func.least(
            literal_column("1.0"), func.greatest(literal_column("0.0"), a)
        )
        c = literal_column("2.0") * func.atan2(
            func.sqrt(a_safe), func.sqrt(literal_column("1.0") - a_safe)
        )
        return func.coalesce(literal_column("6371.0") * c, literal_column("999999.0"))

    async def _geocode_to_coords(
        self,
        search_address: str,
        *,
        address: Optional[str] = None,
        city: Optional[str] = None,
        log_prefix: str = "",
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Геокодирует адрес в координаты (lat, lon).
        Сначала пробует напрямую, затем с суффиксом ', Россия'.
        Возвращает (None, None) если геокодирование не удалось.
        """
        try:
            print(f"[DEBUG] Геокодируем адрес: {search_address}")
            # Пробуем геокодировать как есть
            structured_geo = await geocoder.validate_address(search_address, limit=1)
            if structured_geo and structured_geo.latitude and structured_geo.longitude:
                client_lat = structured_geo.latitude
                client_lon = structured_geo.longitude
                print(
                    f"[DEBUG] Адрес '{search_address}' геокодирован: lat={client_lat}, lon={client_lon}"
                )
                return client_lat, client_lon

            # Если не получилось, пробуем добавить "Россия" для городов
            print(
                f"[DEBUG] Первое геокодирование не удалось, {log_prefix}address={address}, {log_prefix}city={city}"
            )
            search_address_with_country = f"{search_address}, Россия"
            print(
                f"[DEBUG] Повторяем геокодирование с указанием страны: {search_address_with_country}"
            )
            structured_geo = await geocoder.validate_address(
                search_address_with_country, limit=1
            )
            if structured_geo and structured_geo.latitude and structured_geo.longitude:
                client_lat = structured_geo.latitude
                client_lon = structured_geo.longitude
                print(
                    f"[DEBUG] Адрес '{search_address_with_country}' геокодирован: lat={client_lat}, lon={client_lon}"
                )
                return client_lat, client_lon

            print(
                f"[DEBUG] Не удалось геокодировать '{search_address}' и '{search_address_with_country}': координаты не получены"
            )
            return None, None
        except Exception as e:
            print(f"[DEBUG] Ошибка геокодирования '{search_address}': {e}")
            import traceback

            traceback.print_exc()
            return None, None

    async def get_product(
        self,
        product_id: int,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        address: Optional[str] = None,
        city: Optional[str] = None,
    ) -> MarketplaceProductDetail:
        # Логируем параметры запроса для отладки
        print(
            f"[DEBUG] get_product: параметры lat={lat}, lon={lon}, city={city}, address={address}"
        )

        # Если переданы адрес или город, но нет координат - геокодируем адрес
        client_lat = lat
        client_lon = lon
        if (client_lat is None or client_lon is None) and (city or address):
            search_address = address or city
            geo_lat, geo_lon = await self._geocode_to_coords(
                search_address,
                address=address,
                city=city,
                log_prefix="",
            )
            if geo_lat is not None and geo_lon is not None:
                client_lat, client_lon = geo_lat, geo_lon

        current_timestamp = int(datetime.now().timestamp())

        # Если есть координаты клиента, добавляем расчет расстояния
        order_by_list = []

        if client_lat is not None and client_lon is not None:
            # Приоритет: сначала цены с координатами (ближайшие), потом без координат
            distance_for_sort = self._haversine_sql(
                client_lat,
                client_lon,
                prices.c.latitude,
                prices.c.longitude,
            )

            order_by_list = [
                distance_for_sort,
                desc(
                    func.coalesce(prices.c.date_from <= current_timestamp, True)
                    & func.coalesce(current_timestamp < prices.c.date_to, True)
                ),
                desc(prices.c.created_at),
                desc(prices.c.id),
            ]
        else:
            # Если координат клиента нет, используем старую логику
            # Приоритет: сначала цены без адреса (если адрес не задан), потом остальные
            order_by_list = [
                desc(
                    func.coalesce(prices.c.date_from <= current_timestamp, True)
                    & func.coalesce(current_timestamp < prices.c.date_to, True)
                ),
                desc(prices.c.created_at),
                desc(prices.c.id),
            ]

        # Если есть адрес/город для приоритизации, создаем два подзапроса:
        # 1. Цены с совпадающим адресом (приоритет выше)
        # 2. Остальные цены
        # Затем объединяем их через UNION ALL и ранжируем
        # ИСКЛЮЧЕНИЕ: Если передается только city (без address) и есть координаты после геокодирования,
        # то используем обычную логику с сортировкой по расстоянию, а не по текстовому совпадению адреса
        # Также: если есть координаты клиента, но нет city/address - используем сортировку по расстоянию
        use_address_priority = (address or city) and (
            address or not client_lat or not client_lon
        )
        print(
            f"[DEBUG] get_product выбор цены: use_address_priority={use_address_priority}, "
            f"client_lat={client_lat}, client_lon={client_lon}, city={city}, address={address}"
        )
        if use_address_priority:
            search_address_lower = (address or city or "").lower()

            # Подзапрос для цен с совпадающим адресом
            matching_address_prices = (
                select(
                    prices.c.nomenclature.label("nomenclature_id"),
                    prices.c.id.label("price_id"),
                    prices.c.price,
                    prices.c.price_type,
                    prices.c.created_at,
                    prices.c.date_from,
                    prices.c.date_to,
                    prices.c.is_deleted,
                    prices.c.address,
                    prices.c.latitude,
                    prices.c.longitude,
                    cast(literal_column("0"), Integer).label(
                        "address_priority"
                    ),  # Высокий приоритет
                )
                .select_from(
                    prices.join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(
                    prices.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                    prices.c.nomenclature
                    == product_id,  # Фильтруем по конкретному товару
                    prices.c.address.is_not(None),
                    func.lower(prices.c.address).ilike(f"%{search_address_lower}%"),
                )
            )

            # Подзапрос для остальных цен
            other_prices = (
                select(
                    prices.c.nomenclature.label("nomenclature_id"),
                    prices.c.id.label("price_id"),
                    prices.c.price,
                    prices.c.price_type,
                    prices.c.created_at,
                    prices.c.date_from,
                    prices.c.date_to,
                    prices.c.is_deleted,
                    prices.c.address,
                    prices.c.latitude,
                    prices.c.longitude,
                    cast(literal_column("1"), Integer).label(
                        "address_priority"
                    ),  # Низкий приоритет
                )
                .select_from(
                    prices.join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(
                    prices.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                    prices.c.nomenclature
                    == product_id,  # Фильтруем по конкретному товару
                    or_(
                        prices.c.address.is_(None),
                        ~func.lower(prices.c.address).ilike(
                            f"%{search_address_lower}%"
                        ),
                    ),
                )
            )

            # Объединяем через UNION ALL
            all_prices_union = union_all(
                matching_address_prices, other_prices
            ).subquery()

            # Ранжируем объединенные цены
            # Если есть координаты, добавляем расстояние в сортировку
            union_order_by = [
                all_prices_union.c.address_priority
            ]  # Сначала совпадающие адреса
            if client_lat is not None and client_lon is not None:
                # Добавляем расчет расстояния для UNION запроса
                union_distance_for_sort = self._haversine_sql(
                    client_lat,
                    client_lon,
                    all_prices_union.c.latitude,
                    all_prices_union.c.longitude,
                )
                union_order_by.append(union_distance_for_sort)  # Потом по расстоянию

            # Добавляем остальную сортировку
            union_order_by.extend(
                [
                    desc(
                        func.coalesce(
                            all_prices_union.c.date_from <= current_timestamp, True
                        )
                        & func.coalesce(
                            current_timestamp < all_prices_union.c.date_to, True
                        )
                    ),
                    desc(all_prices_union.c.created_at),
                    desc(all_prices_union.c.price_id),
                ]
            )

            ranked_prices_subquery = select(
                all_prices_union.c.nomenclature_id,
                all_prices_union.c.price_id,
                all_prices_union.c.price,
                all_prices_union.c.price_type,
                all_prices_union.c.created_at,
                all_prices_union.c.date_from,
                all_prices_union.c.date_to,
                all_prices_union.c.is_deleted,
                all_prices_union.c.address,
                all_prices_union.c.latitude,
                all_prices_union.c.longitude,
                func.row_number()
                .over(
                    partition_by=all_prices_union.c.nomenclature_id,
                    order_by=union_order_by,
                )
                .label("rn"),
            ).subquery()
        elif (
            client_lat is not None
            and client_lon is not None
            and (not address or not address.strip())
            and (not city or not city.strip())
        ) or (city and client_lat is not None and client_lon is not None):
            # Если есть координаты клиента (с city или без address/city), используем сортировку по расстоянию
            # Это позволяет выбирать ближайшие цены по координатам
            print(
                f"[DEBUG] get_product: выбор цены по расстоянию, координаты lat={client_lat}, lon={client_lon}, city={city}, address={address}"
            )
            where_conditions = [
                prices.c.is_deleted.is_not(True),
                price_types.c.name == "chatting",
                prices.c.nomenclature == product_id,
            ]

            ranked_prices_subquery = (
                select(
                    prices.c.nomenclature.label("nomenclature_id"),
                    prices.c.id.label("price_id"),
                    prices.c.price,
                    prices.c.price_type,
                    prices.c.created_at,
                    prices.c.date_from,
                    prices.c.date_to,
                    prices.c.is_deleted,
                    prices.c.address,
                    prices.c.latitude,
                    prices.c.longitude,
                    func.row_number()
                    .over(
                        partition_by=prices.c.nomenclature,
                        order_by=order_by_list,
                    )
                    .label("rn"),
                )
                .select_from(
                    prices.join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(and_(*where_conditions))
                .subquery()
            )
        else:
            # Если нет адреса для приоритизации, используем обычный запрос
            # Если нет координат и нет адреса - выбираем только цены БЕЗ адреса
            where_conditions = [
                prices.c.is_deleted.is_not(True),
                price_types.c.name == "chatting",
            ]

            # Если нет координат и нет адреса - фильтруем только цены без адреса
            if client_lat is None and client_lon is None and not address and not city:
                where_conditions.append(prices.c.address.is_(None))

            ranked_prices_subquery = (
                select(
                    prices.c.nomenclature.label("nomenclature_id"),
                    prices.c.id.label("price_id"),
                    prices.c.price,
                    prices.c.price_type,
                    prices.c.created_at,
                    prices.c.date_from,
                    prices.c.date_to,
                    prices.c.is_deleted,
                    prices.c.address,
                    prices.c.latitude,
                    prices.c.longitude,
                    func.row_number()
                    .over(
                        partition_by=prices.c.nomenclature,
                        order_by=order_by_list,
                    )
                    .label("rn"),
                )
                .select_from(
                    prices.join(price_types, price_types.c.id == prices.c.price_type)
                )
                .where(and_(*where_conditions))
                .subquery()
            )

        active_prices_subquery = (
            select(ranked_prices_subquery)
            .where(ranked_prices_subquery.c.rn == 1)
            .subquery()
        )

        total_sold_subquery = (
            select(
                docs_sales_goods.c.nomenclature,
                func.count(docs_sales_goods.c.id).label("total_sold"),
            )
            .group_by(docs_sales_goods.c.nomenclature)
            .subquery()
        )

        # Основной запрос для получения базовой информации о товаре
        query = (
            select(
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.description_short,
                nomenclature.c.description_long,
                nomenclature.c.code,
                nomenclature.c.cashbox,
                nomenclature.c.created_at,
                nomenclature.c.updated_at,
                nomenclature.c.tags,
                nomenclature.c.type,
                nomenclature.c.seo_title,
                nomenclature.c.seo_description,
                nomenclature.c.seo_keywords,
                nomenclature.c.production_time_min_from,
                nomenclature.c.production_time_min_to,
                units.c.convent_national_view.label("unit_name"),
                categories.c.name.label("category_name"),
                manufacturers.c.name.label("manufacturer_name"),
                active_prices_subquery.c.price,
                price_types.c.name.label("price_type"),
                active_prices_subquery.c.address.label("price_address"),
                active_prices_subquery.c.latitude.label("price_latitude"),
                active_prices_subquery.c.longitude.label("price_longitude"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""),
                    cboxes.c.name,
                ).label("seller_name"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("seller_photo"),
                cboxes.c.seller_description.label("seller_description"),
                # Для отправки в Telegram используем owner_id, а не chat_id
                users.c.owner_id.label("recipient_id"),
                users.c.username.label("username"),
                marketplace_rating_aggregates.c.avg_rating.label("rating"),
                nomenclature.c.rating.label("global_rating"),
                marketplace_rating_aggregates.c.reviews_count.label("reviews_count"),
                func.array_agg(func.distinct(pictures.c.id))
                .filter(pictures.c.id.is_not(None))
                .label("images"),
                func.array_agg(func.distinct(nomenclature_barcodes.c.code))
                .filter(nomenclature_barcodes.c.code.is_not(None))
                .label("barcodes"),
                func.coalesce(total_sold_subquery.c.total_sold, 0).label("total_sold"),
            )
            .select_from(nomenclature)
            .join(units, units.c.id == nomenclature.c.unit, isouter=True)
            .join(categories, categories.c.id == nomenclature.c.category, isouter=True)
            .join(
                manufacturers,
                manufacturers.c.id == nomenclature.c.manufacturer,
                isouter=True,
            )
            .join(
                active_prices_subquery,
                active_prices_subquery.c.nomenclature_id == nomenclature.c.id,
            )
            .join(price_types, price_types.c.id == active_prices_subquery.c.price_type)
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin, isouter=True)
            .join(
                pictures,
                and_(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id == nomenclature.c.id,
                    pictures.c.is_deleted.is_not(True),
                ),
                isouter=True,
            )
            .join(
                nomenclature_barcodes,
                nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
            .join(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                    marketplace_rating_aggregates.c.entity_type == "nomenclature",
                ),
                isouter=True,
            )
            .join(
                total_sold_subquery,
                total_sold_subquery.c.nomenclature == nomenclature.c.id,
                isouter=True,
            )
            .where(
                and_(
                    nomenclature.c.id == product_id,
                    nomenclature.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                )
            )
            .group_by(
                nomenclature.c.id,
                units.c.convent_national_view,
                categories.c.name,
                manufacturers.c.name,
                active_prices_subquery.c.price,
                price_types.c.name,
                active_prices_subquery.c.address,
                active_prices_subquery.c.latitude,
                active_prices_subquery.c.longitude,
                cboxes.c.seller_name,
                cboxes.c.name,
                cboxes.c.seller_photo,
                users.c.photo,
                cboxes.c.seller_description,
                users.c.owner_id,
                users.c.username,
                nomenclature.c.rating,
                marketplace_rating_aggregates.c.avg_rating,
                marketplace_rating_aggregates.c.reviews_count,
                total_sold_subquery.c.total_sold,
            )
        )

        row = await database.fetch_one(query)
        if not row:
            raise HTTPException(status_code=404, detail="Товар не найден")

        product = dict(row)

        # Отдельный запрос для получения складов с остатками
        # ОПТИМИЗАЦИЯ: Используем подзапрос с MAX вместо прямого запроса - получаем только последние остатки
        warehouses_query = (
            select(
                warehouses.c.id.label("warehouse_id"),
                warehouses.c.name.label("warehouse_name"),
                warehouses.c.address.label("warehouse_address"),
                warehouses.c.latitude,
                warehouses.c.longitude,
                warehouse_balances_latest.c.current_amount,
                warehouse_balances_latest.c.organization_id,
            )
            .select_from(
                warehouse_balances_latest.join(
                    warehouses,
                    and_(
                        warehouses.c.id == warehouse_balances_latest.c.warehouse_id,
                        warehouses.c.is_deleted.is_not(True),
                    ),
                )
            )
            .where(
                and_(
                    warehouse_balances_latest.c.nomenclature_id == product_id,
                    warehouse_balances_latest.c.current_amount
                    > 0,  # Только склады с остатками
                )
            )
        )

        warehouses_rows = await database.fetch_all(warehouses_query)

        total_amount = 0
        available_warehouses = []
        for wh_row in warehouses_rows:
            wh_dict = dict(wh_row)
            total_amount += wh_dict["current_amount"] or 0
            available_warehouses.append(
                AvailableWarehouse(
                    warehouse_id=wh_dict["warehouse_id"],
                    organization_id=wh_dict["organization_id"],
                    warehouse_name=wh_dict["warehouse_name"],
                    warehouse_address=wh_dict["warehouse_address"],
                    latitude=wh_dict["latitude"],
                    longitude=wh_dict["longitude"],
                    current_amount=wh_dict["current_amount"],
                    distance_to_client=self._count_distance_to_client(
                        None, None, wh_dict["latitude"], wh_dict["longitude"]
                    ),
                )
            )

        product["available_warehouses"] = available_warehouses or None
        product["current_amount"] = total_amount

        # Вариации товаров
        # 1. Получаем все группы, к которым принадлежит товар
        group_query = (
            select(
                nomenclature_groups_value.c.group_id,
                nomenclature_groups.c.name.label("group_name"),
            )
            .select_from(nomenclature_groups_value)
            .join(
                nomenclature_groups,
                nomenclature_groups.c.id == nomenclature_groups_value.c.group_id,
            )
            .where(nomenclature_groups_value.c.nomenclature_id == product_id)
        )

        groups = await database.fetch_all(group_query)

        nomenclatures_result = []

        # Получаем все вариации одним запросом вместо множества запросов (избегаем N+1)
        variations_by_group = {}
        if groups:
            group_ids = [group["group_id"] for group in groups]

            # Загружаем все вариации для всех групп сразу
            all_variations_query = (
                select(
                    nomenclature_groups_value.c.group_id,
                    nomenclature.c.id,
                    nomenclature.c.name,
                    nomenclature_groups_value.c.is_main,
                )
                .select_from(nomenclature_groups_value)
                .join(
                    nomenclature,
                    nomenclature.c.id == nomenclature_groups_value.c.nomenclature_id,
                )
                .where(nomenclature_groups_value.c.group_id.in_(group_ids))
            )
            all_variations = await database.fetch_all(all_variations_query)

            # Раскладываем вариации по группам
            for variation in all_variations:
                group_id = variation["group_id"]
                if group_id not in variations_by_group:
                    variations_by_group[group_id] = []
                variations_by_group[group_id].append(
                    {
                        "id": variation["id"],
                        "name": variation["name"],
                        "is_main": variation["is_main"],
                    }
                )

        # Используем уже загруженные вариации для каждой группы
        for group in groups:
            group_id = group["group_id"]
            group_name = group["group_name"]

            variations = variations_by_group.get(group_id, [])

            items = [
                {"id": v["id"], "name": v["name"], "is_main": v["is_main"]}
                for v in variations
            ]

            nomenclatures_result.append({"group_name": group_name, "items": items})

        product["nomenclatures"] = nomenclatures_result or None

        # Фото - преобразуем ID фото в публичные URL
        if (
            product.get("images")
            and isinstance(product["images"], list)
            and any(product["images"])
        ):
            product["images"] = [
                self.__build_picture_url(picture_id)
                for picture_id in product["images"]
                if picture_id is not None
            ]
            # Убираем None значения
            product["images"] = [
                url for url in product["images"] if url is not None
            ] or None
        video_rows = await database.fetch_all(
            select(nomenclature_videos)
            .where(nomenclature_videos.c.nomenclature_id == product_id)
            .order_by(nomenclature_videos.c.id.asc())
        )
        product["videos"] = [dict(r) for r in video_rows]
        # Селлер
        if product["seller_photo"]:
            product["seller_photo"] = self.__transform_photo_route(
                product["seller_photo"]
            )

        # Штрихкоды
        product["barcodes"] = [b for b in (product["barcodes"] or []) if b]

        # Поле cashbox_id
        product["cashbox_id"] = product["cashbox"]
        # Формируем отдельный список с главным админом кассы
        recipient_id = product.pop("recipient_id", None)
        admin_username = product.pop("username", None)
        if admin_username:
            admin_username = admin_username.lstrip("@")
        product["user_admin"] = (
            [
                MarketplaceProductUserAdmin(
                    recipient_id=str(recipient_id),
                    username=admin_username,
                )
            ]
            if recipient_id
            else None
        )

        product["listing_pos"] = 1
        product["listing_page"] = 1
        product["is_ad_pos"] = False
        product["variations"] = []

        # distance — расстояние до ближайшего склада
        if product["available_warehouses"]:
            product["distance"] = min(
                product["available_warehouses"],
                key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0),
            ).distance_to_client
        else:
            product["distance"] = None

        # Добавляем атрибуты
        attrs = await database.fetch_all(
            select(
                nomenclature_attributes.c.name, nomenclature_attributes_value.c.value
            )
            .select_from(nomenclature_attributes_value)
            .join(
                nomenclature_attributes,
                nomenclature_attributes.c.id
                == nomenclature_attributes_value.c.attribute_id,
            )
            .where(nomenclature_attributes_value.c.nomenclature_id == product_id)
        )
        product_attributes = [
            MarketplaceProductAttribute(name=a.name, value=a.value) for a in attrs
        ]

        return MarketplaceProductDetail(**product, attributes=product_attributes)

    async def _resolve_client_coordinates(
        self,
        request: MarketplaceProductsRequest,
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Определяет координаты клиента.
        Если координаты не переданы, пытается получить их через геокодинг по адресу/городу.
        Возвращает (lat, lon), может вернуть (None, None), если координаты определить не удалось.
        """
        # Засекаем время подготовки координат (включая геокодинг)
        start_time = time.perf_counter()
        try:
            # Логируем параметры запроса для отладки
            print(
                f"[DEBUG] get_products: параметры lat={request.lat}, lon={request.lon}, city={request.city}, address={request.address}"
            )

            # Если переданы адрес или город, но нет координат - геокодируем адрес
            client_lat = request.lat
            client_lon = request.lon
            if (client_lat is None or client_lon is None) and (
                request.city or request.address
            ):
                search_address = request.address or request.city
                geo_lat, geo_lon = await self._geocode_to_coords(
                    search_address,
                    address=request.address,
                    city=request.city,
                    log_prefix="request.",
                )
                if geo_lat is not None and geo_lon is not None:
                    client_lat, client_lon = geo_lat, geo_lon

            return client_lat, client_lon
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _resolve_client_coordinates: {elapsed:.4f}s")

    def _build_active_prices_subquery(
        self,
        request: MarketplaceProductsRequest,
        client_lat: Optional[float],
        client_lon: Optional[float],
    ):
        """
        Формирует подзапрос для выбора актуальной цены по каждой номенклатуре.
        Учитывает:
        - приоритет адреса,
        - расстояние до клиента (Haversine),
        - радиус действия цены (если задан и клиент внутри радиуса — наивысший приоритет).
        """
        start_time = time.perf_counter()
        try:
            current_timestamp = int(datetime.now().timestamp())
            order_by_list = []

            # --- Базовый порядок сортировки (без радиуса) ---
            if client_lat is not None and client_lon is not None:
                distance_for_sort = self._haversine_sql(
                    client_lat, client_lon, prices.c.latitude, prices.c.longitude
                )
                order_by_list = [
                    distance_for_sort,
                    desc(
                        func.coalesce(prices.c.date_from <= current_timestamp, True)
                        & func.coalesce(current_timestamp < prices.c.date_to, True)
                    ),
                    desc(prices.c.created_at),
                    desc(prices.c.id),
                ]
            else:
                order_by_list = [
                    desc(
                        func.coalesce(prices.c.date_from <= current_timestamp, True)
                        & func.coalesce(current_timestamp < prices.c.date_to, True)
                    ),
                    desc(prices.c.created_at),
                    desc(prices.c.id),
                ]

            # --- Если есть координаты клиента, добавляем приоритет радиуса ---
            radius_priority_expr = literal_column("0")
            if client_lat is not None and client_lon is not None:
                distance_expr = self._haversine_sql(
                    client_lat, client_lon, prices.c.latitude, prices.c.longitude
                )
                radius_priority_expr = case(
                    (
                        and_(
                            prices.c.radius.is_not(None),
                            distance_expr <= prices.c.radius,
                        ),
                        literal_column("-1"),
                    ),
                    else_=literal_column("0"),
                ).label("radius_priority")

            # --- Определяем, нужно ли использовать приоритет адреса ---
            use_address_priority = (request.address or request.city) and (
                request.address or not client_lat or not client_lon
            )

            if use_address_priority:
                search_address_lower = (request.address or request.city or "").lower()

                # Цены с совпадающим адресом
                matching_address_prices = (
                    select(
                        prices.c.nomenclature.label("nomenclature_id"),
                        prices.c.id.label("price_id"),
                        prices.c.price,
                        prices.c.price_type,
                        prices.c.created_at,
                        prices.c.date_from,
                        prices.c.date_to,
                        prices.c.is_deleted,
                        prices.c.address,
                        prices.c.latitude,
                        prices.c.longitude,
                        prices.c.radius,
                        prices.c.hide_outside_radius,
                        cast(literal_column("0"), Integer).label("address_priority"),
                        radius_priority_expr,
                    )
                    .select_from(
                        prices.join(
                            price_types, price_types.c.id == prices.c.price_type
                        )
                    )
                    .where(
                        prices.c.is_deleted.is_not(True),
                        price_types.c.name == "chatting",
                        prices.c.address.is_not(None),
                        func.lower(prices.c.address).ilike(f"%{search_address_lower}%"),
                    )
                )

                # Остальные цены
                other_prices = (
                    select(
                        prices.c.nomenclature.label("nomenclature_id"),
                        prices.c.id.label("price_id"),
                        prices.c.price,
                        prices.c.price_type,
                        prices.c.created_at,
                        prices.c.date_from,
                        prices.c.date_to,
                        prices.c.is_deleted,
                        prices.c.address,
                        prices.c.latitude,
                        prices.c.longitude,
                        prices.c.radius,
                        prices.c.hide_outside_radius,
                        cast(literal_column("1"), Integer).label("address_priority"),
                        radius_priority_expr,
                    )
                    .select_from(
                        prices.join(
                            price_types, price_types.c.id == prices.c.price_type
                        )
                    )
                    .where(
                        prices.c.is_deleted.is_not(True),
                        price_types.c.name == "chatting",
                        or_(
                            prices.c.address.is_(None),
                            ~func.lower(prices.c.address).ilike(
                                f"%{search_address_lower}%"
                            ),
                        ),
                    )
                )

                all_prices_union = union_all(
                    matching_address_prices, other_prices
                ).subquery()

                # Сортировка для UNION
                union_order_by = [
                    all_prices_union.c.radius_priority,
                    all_prices_union.c.address_priority,
                ]
                if client_lat is not None and client_lon is not None:
                    union_distance = self._haversine_sql(
                        client_lat,
                        client_lon,
                        all_prices_union.c.latitude,
                        all_prices_union.c.longitude,
                    )
                    union_order_by.append(union_distance)
                union_order_by.extend(
                    [
                        desc(
                            func.coalesce(
                                all_prices_union.c.date_from <= current_timestamp, True
                            )
                            & func.coalesce(
                                current_timestamp < all_prices_union.c.date_to, True
                            )
                        ),
                        desc(all_prices_union.c.created_at),
                        desc(all_prices_union.c.price_id),
                    ]
                )

                ranked_prices_subquery = select(
                    all_prices_union.c.nomenclature_id,
                    all_prices_union.c.price_id,
                    all_prices_union.c.price,
                    all_prices_union.c.price_type,
                    all_prices_union.c.created_at,
                    all_prices_union.c.date_from,
                    all_prices_union.c.date_to,
                    all_prices_union.c.is_deleted,
                    all_prices_union.c.address,
                    all_prices_union.c.latitude,
                    all_prices_union.c.longitude,
                    all_prices_union.c.radius,
                    all_prices_union.c.hide_outside_radius,
                    func.row_number()
                    .over(
                        partition_by=all_prices_union.c.nomenclature_id,
                        order_by=union_order_by,
                    )
                    .label("rn"),
                ).subquery()

            elif (
                client_lat is not None
                and client_lon is not None
                and (not request.address or not request.address.strip())
                and (not request.city or not request.city.strip())
            ) or (request.city and client_lat is not None and client_lon is not None):
                # Сортировка по расстоянию + радиусу
                where_conditions = [
                    prices.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                ]
                order_with_radius = [radius_priority_expr] + order_by_list

                ranked_prices_subquery = (
                    select(
                        prices.c.nomenclature.label("nomenclature_id"),
                        prices.c.id.label("price_id"),
                        prices.c.price,
                        prices.c.price_type,
                        prices.c.created_at,
                        prices.c.date_from,
                        prices.c.date_to,
                        prices.c.is_deleted,
                        prices.c.address,
                        prices.c.latitude,
                        prices.c.longitude,
                        prices.c.radius,
                        prices.c.hide_outside_radius,
                        func.row_number()
                        .over(
                            partition_by=prices.c.nomenclature,
                            order_by=order_with_radius,
                        )
                        .label("rn"),
                    )
                    .select_from(
                        prices.join(
                            price_types, price_types.c.id == prices.c.price_type
                        )
                    )
                    .where(and_(*where_conditions))
                    .subquery()
                )
            else:
                # Обычный запрос (без приоритета адреса)
                where_conditions = [
                    prices.c.is_deleted.is_not(True),
                    price_types.c.name == "chatting",
                ]
                if (
                    client_lat is None
                    and client_lon is None
                    and not request.address
                    and not request.city
                ):
                    where_conditions.append(prices.c.address.is_(None))

                order_with_radius = (
                    [radius_priority_expr] + order_by_list
                    if client_lat is not None
                    else order_by_list
                )

                ranked_prices_subquery = (
                    select(
                        prices.c.nomenclature.label("nomenclature_id"),
                        prices.c.id.label("price_id"),
                        prices.c.price,
                        prices.c.price_type,
                        prices.c.created_at,
                        prices.c.date_from,
                        prices.c.date_to,
                        prices.c.is_deleted,
                        prices.c.address,
                        prices.c.latitude,
                        prices.c.longitude,
                        prices.c.radius,
                        prices.c.hide_outside_radius,
                        func.row_number()
                        .over(
                            partition_by=prices.c.nomenclature,
                            order_by=order_with_radius,
                        )
                        .label("rn"),
                    )
                    .select_from(
                        prices.join(
                            price_types, price_types.c.id == prices.c.price_type
                        )
                    )
                    .where(and_(*where_conditions))
                    .subquery()
                )

            active_prices_subquery = (
                select(ranked_prices_subquery)
                .where(ranked_prices_subquery.c.rn == 1)
                .subquery()
            )
            return active_prices_subquery
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _build_active_prices_subquery: {elapsed:.4f}s")

    def _build_total_sold_subquery(self, active_prices_subquery):
        """
        Формирует подзапрос для подсчета количества продаж.
        Считает только по тем товарам, у которых есть актуальная цена.
        """
        # Засекаем время подготовки подзапроса продаж
        start_time = time.perf_counter()
        try:
            total_sold_subquery = (
                select(
                    docs_sales_goods.c.nomenclature,
                    func.count(docs_sales_goods.c.id).label("total_sold"),
                )
                .where(
                    docs_sales_goods.c.nomenclature.in_(
                        select(active_prices_subquery.c.nomenclature_id)
                    )
                )
                .group_by(docs_sales_goods.c.nomenclature)
                .subquery()
            )

            return total_sold_subquery
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _build_total_sold_subquery: {elapsed:.4f}s")

    def _build_attrs_subquery(self, request: MarketplaceProductsRequest):
        """
        Формирует подзапрос для поиска по атрибутам номенклатуры.
        Возвращает None, если фильтр по атрибутам не задан.
        """
        # Засекаем время подготовки подзапроса по атрибутам
        start_time = time.perf_counter()
        try:
            # Создаём подзапрос только если есть значение для поиска по атрибутам
            attrs_subquery = None
            if request.nomenclature_attributes:
                attrs_subquery = (
                    select(nomenclature_attributes_value.c.nomenclature_id)
                    .select_from(
                        nomenclature_attributes_value.join(
                            nomenclature_attributes,
                            nomenclature_attributes.c.id
                            == nomenclature_attributes_value.c.attribute_id,
                        )
                    )
                    .where(
                        func.lower(nomenclature_attributes_value.c.value).ilike(
                            f"%{request.nomenclature_attributes.lower()}%"
                        )
                    )
                    .distinct()
                    .subquery()
                )

            return attrs_subquery
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _build_attrs_subquery: {elapsed:.4f}s")

    def _build_stock_subqueries(self, request: MarketplaceProductsRequest):
        """
        Готовит подзапросы и алиасы для работы с остатками по складам.
        Возвращает:
        - wb_latest: последние остатки по каждой паре (организация, склад, номенклатура)
        - stock_subquery: суммарный остаток по товару
        - wh_bal: алиас таблицы складов
        """
        # Засекаем время подготовки подзапросов по остаткам
        start_time = time.perf_counter()
        try:
            # 1) Берём последнюю запись по каждой паре (организация, склад, номенклатура)
            wb_latest = select(
                warehouse_balances_latest.c.organization_id,
                warehouse_balances_latest.c.warehouse_id,
                warehouse_balances_latest.c.nomenclature_id,
                warehouse_balances_latest.c.current_amount,
            ).subquery()
            # 2) Подсчёт суммарного остатка по товару (только положительные остатки)
            stock_subquery = (
                select(
                    wb_latest.c.nomenclature_id.label("nomenclature_id"),
                    func.sum(func.greatest(wb_latest.c.current_amount, 0)).label(
                        "current_amount"
                    ),
                )
                .select_from(wb_latest)
                .group_by(wb_latest.c.nomenclature_id)
                .subquery()
            )

            # 3) Подготовка алиаса для складов (используется в отдельном запросе для складов)
            wh_bal = warehouses.alias("wh_bal")

            # 4) Подзапрос для определения, есть ли у товара склад в указанном городе
            # Примечание: логика сохранена, но подзапрос используется ниже в Python-логике
            if request.city:
                _ = (
                    select(
                        wb_latest.c.nomenclature_id.label("nomenclature_id"),
                        func.bool_or(
                            func.lower(wh_bal.c.address).ilike(
                                f"%{request.city.lower()}%"
                            )
                        ).label("has_city_warehouse"),
                    )
                    .select_from(
                        wb_latest.join(
                            wh_bal,
                            and_(
                                wh_bal.c.id == wb_latest.c.warehouse_id,
                                wh_bal.c.is_deleted.is_not(True),
                            ),
                        )
                    )
                    .where(wh_bal.c.address.is_not(None))
                    .group_by(wb_latest.c.nomenclature_id)
                    .subquery()
                )

            return wb_latest, stock_subquery, wh_bal
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _build_stock_subqueries: {elapsed:.4f}s")

    async def _build_products_query(
        self,
        request: MarketplaceProductsRequest,
        active_prices_subquery,
        total_sold_subquery,
        attrs_subquery,
        stock_subquery,
        wb_latest,
        wh_bal,
        client_lat: Optional[float],
        client_lon: Optional[float],
        city: Optional[str],
        seller_id: Optional[int],
        address: Optional[str],
    ):
        """
        Собирает основной SQL-запрос по товарам.
        Возвращает сам запрос и список условий фильтрации (conditions),
        которые пригодятся для подсчета общего количества.
        """
        # Засекаем время построения основного запроса по товарам
        start_time = time.perf_counter()
        try:
            # --- Приоритизация (город/селлер/адрес) в SQL ---
            if city:
                city_lower = city.lower()
                has_city_condition = (
                    select(literal_column("1"))
                    .select_from(
                        wb_latest.join(
                            wh_bal,
                            and_(
                                wh_bal.c.id == wb_latest.c.warehouse_id,
                                wh_bal.c.is_deleted.is_not(True),
                            ),
                        )
                    )
                    .where(
                        and_(
                            wb_latest.c.nomenclature_id == nomenclature.c.id,
                            wh_bal.c.address.is_not(None),
                            func.lower(wh_bal.c.address).ilike(f"%{city_lower}%"),
                        )
                    )
                    .exists()
                )
            else:
                has_city_condition = literal_column("FALSE")

            if seller_id:
                is_seller_condition = cboxes.c.id == seller_id
            else:
                is_seller_condition = literal_column("FALSE")

            has_address_condition = literal_column("FALSE")
            if (address or city) and (not client_lat or not client_lon):
                address_lower = (address or city or "").lower()
                has_address_condition = and_(
                    active_prices_subquery.c.address.is_not(None),
                    active_prices_subquery.c.address != "",
                    func.lower(active_prices_subquery.c.address).ilike(
                        f"%{address_lower}%"
                    ),
                )

            priority_expr = case(
                (
                    and_(
                        has_city_condition, is_seller_condition, has_address_condition
                    ),
                    literal_column("0"),
                ),
                (and_(has_city_condition, is_seller_condition), literal_column("1")),
                (and_(has_city_condition, has_address_condition), literal_column("2")),
                (has_city_condition, literal_column("3")),
                (and_(is_seller_condition, has_address_condition), literal_column("4")),
                (is_seller_condition, literal_column("5")),
                (has_address_condition, literal_column("6")),
                else_=literal_column("7"),
            ).label("sort_priority")
            # Формируем список колонок для select
            select_columns = [
                nomenclature.c.id,
                nomenclature.c.name,
                nomenclature.c.description_short,
                nomenclature.c.description_long,
                nomenclature.c.code,
                nomenclature.c.cashbox,
                nomenclature.c.created_at,
                nomenclature.c.updated_at,
                nomenclature.c.tags,
                nomenclature.c.type,
                nomenclature.c.production_time_min_from,
                nomenclature.c.production_time_min_to,
                units.c.convent_national_view.label("unit_name"),
                categories.c.name.label("category_name"),
                manufacturers.c.name.label("manufacturer_name"),
                active_prices_subquery.c.price,
                price_types.c.name.label("price_type"),
                active_prices_subquery.c.address.label("price_address"),
                active_prices_subquery.c.latitude.label("price_latitude"),
                active_prices_subquery.c.longitude.label("price_longitude"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""),
                    cboxes.c.name,
                ).label("seller_name"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_photo, ""),
                    users.c.photo,
                ).label("seller_photo"),
                cboxes.c.seller_description.label("seller_description"),
                # Для отправки в Telegram используем owner_id, а не chat_id
                users.c.owner_id.label("recipient_id"),
                users.c.username.label("username"),
                marketplace_rating_aggregates.c.avg_rating.label("rating"),
                nomenclature.c.rating.label("global_rating"),
                marketplace_rating_aggregates.c.reviews_count.label("reviews_count"),
                func.array_agg(func.distinct(pictures.c.id))
                .filter(pictures.c.id.is_not(None))
                .label("images"),
                func.array_agg(func.distinct(nomenclature_barcodes.c.code))
                .filter(nomenclature_barcodes.c.code.is_not(None))
                .label("barcodes"),
                # суммарный остаток по всем складам (минимум 0)
                func.coalesce(stock_subquery.c.current_amount, 0).label(
                    "current_amount"
                ),
                func.coalesce(total_sold_subquery.c.total_sold, 0).label("total_sold"),
                # Склады получаем отдельным запросом после основного - это быстрее
                literal_column("NULL::jsonb[]").label("available_warehouses"),
                # Добавляем cashbox_id для определения приоритета в Python
                nomenclature.c.cashbox.label("cashbox_id"),
                # Приоритет для сортировки
                priority_expr,
            ]

            query = (
                select(*select_columns)
                .select_from(nomenclature)
                .join(units, units.c.id == nomenclature.c.unit, isouter=True)
                .join(
                    categories, categories.c.id == nomenclature.c.category, isouter=True
                )
                .join(
                    manufacturers,
                    manufacturers.c.id == nomenclature.c.manufacturer,
                    isouter=True,
                )
                .join(
                    active_prices_subquery,
                    active_prices_subquery.c.nomenclature_id == nomenclature.c.id,
                )
                .join(
                    price_types,
                    price_types.c.id == active_prices_subquery.c.price_type,
                )
                .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
                .join(users, users.c.id == cboxes.c.admin, isouter=True)
                .join(
                    pictures,
                    and_(
                        pictures.c.entity == "nomenclature",
                        pictures.c.entity_id == nomenclature.c.id,
                        pictures.c.is_deleted.is_not(True),
                    ),
                    isouter=True,
                )
                .join(
                    nomenclature_barcodes,
                    nomenclature_barcodes.c.nomenclature_id == nomenclature.c.id,
                    isouter=True,
                )
                .join(
                    marketplace_rating_aggregates,
                    and_(
                        marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                        marketplace_rating_aggregates.c.entity_type == "nomenclature",
                    ),
                    isouter=True,
                )
                .join(
                    stock_subquery,
                    stock_subquery.c.nomenclature_id == nomenclature.c.id,
                    isouter=True,
                )
                .join(
                    total_sold_subquery,
                    total_sold_subquery.c.nomenclature == nomenclature.c.id,
                    isouter=True,
                )
            )

            if request.nomenclature_attributes:
                query = query.join(
                    attrs_subquery,
                    attrs_subquery.c.nomenclature_id == nomenclature.c.id,
                    isouter=True,
                )

            # --- Условия фильтрации ---
            conditions = [
                nomenclature.c.is_deleted.is_not(True),
                price_types.c.name == "chatting",
            ]

            if request.category:
                conditions.append(categories.c.name.ilike(f"%{request.category}%"))
            if request.manufacturer:
                conditions.append(
                    manufacturers.c.name.ilike(f"%{request.manufacturer}%")
                )
            if request.min_price is not None:
                conditions.append(active_prices_subquery.c.price >= request.min_price)
            if request.max_price is not None:
                conditions.append(active_prices_subquery.c.price <= request.max_price)
            if request.in_stock:
                # фильтруем по суммарному остатку
                conditions.append(stock_subquery.c.current_amount > 0)
            if request.rating_from is not None:
                conditions.append(
                    marketplace_rating_aggregates.c.avg_rating
                    >= request.avg_rating_from
                )
            if request.rating_to is not None:
                conditions.append(
                    marketplace_rating_aggregates.c.avg_rating <= request.avg_rating_to
                )
            if request.global_rating_from is not None:
                conditions.append(nomenclature.c.rating >= request.global_rating_from)
            if request.global_rating_to is not None:
                conditions.append(nomenclature.c.rating <= request.global_rating_to)
            if request.seller_name:
                conditions.append(
                    func.lower(cboxes.c.name).ilike(f"%{request.seller_name.lower()}%")
                )
            if request.seller_id:
                conditions.append(cboxes.c.id == request.seller_id)
            if request.seller_phone:
                conditions.append(
                    func.lower(users.c.phone_number).ilike(
                        f"%{request.seller_phone.lower()}%"
                    )
                )

            if request.id:
                if request.id.isdigit():
                    conditions.append(nomenclature.c.id == int(request.id))

            if request.name:
                conditions.append(
                    func.lower(nomenclature.c.name).ilike(f"%{request.name.lower()}%")
                )

            if request.description_long:
                conditions.append(
                    func.lower(nomenclature.c.description_long).ilike(
                        f"%{request.description_long.lower()}%"
                    )
                )

            if request.seo_title:
                conditions.append(
                    func.lower(nomenclature.c.seo_title).ilike(
                        f"%{request.seo_title.lower()}%"
                    )
                )

            if request.seo_description:
                conditions.append(
                    func.lower(nomenclature.c.seo_description).ilike(
                        f"%{request.seo_description.lower()}%"
                    )
                )

            if request.seo_keywords:
                # func.unnest(Book.categories).column_valued()
                column_valued = func.unnest(nomenclature.c.seo_keywords).column_valued(
                    "unnested_keywords"
                )
                # conditions.append(column_valued.ilike(f"%{request.seo_keywords.lower()}%"))
                conditions.append(
                    select(column_valued)
                    .where(column_valued.ilike(f"%{request.seo_keywords.lower()}%"))
                    .exists()
                )

            if request.nomenclature_attributes:
                conditions.append(nomenclature.c.id == attrs_subquery.c.nomenclature_id)
            if (
                request.apply_radius_filter
                and client_lat is not None
                and client_lon is not None
            ):
                distance_expr = self._haversine_sql(
                    client_lat,
                    client_lon,
                    active_prices_subquery.c.latitude,
                    active_prices_subquery.c.longitude,
                )
                radius_filter_condition = or_(
                    active_prices_subquery.c.hide_outside_radius.is_(False),
                    and_(
                        active_prices_subquery.c.radius.is_not(None),
                        distance_expr <= active_prices_subquery.c.radius,
                    ),
                )
                conditions.append(radius_filter_condition)
            if request.global_category_id:
                # Получаем все ID категорий (включая дочерние) рекурсивно
                all_category_ids = await MarketplacePublicCategoriesService._get_all_category_ids_recursive(
                    request.global_category_id
                )
                if all_category_ids:
                    conditions.append(
                        nomenclature.c.global_category_id.in_(all_category_ids)
                    )

            query = query.where(and_(*conditions))

            # --- GROUP BY — только по неизменяемым полям, без current_amount из balances ---
            group_by_fields = [
                nomenclature.c.id,
                units.c.convent_national_view,
                categories.c.name,
                manufacturers.c.name,
                active_prices_subquery.c.price,
                price_types.c.name,
                active_prices_subquery.c.address,
                active_prices_subquery.c.latitude,
                active_prices_subquery.c.longitude,
                cboxes.c.seller_name,
                cboxes.c.name,
                cboxes.c.seller_photo,
                users.c.photo,
                cboxes.c.seller_description,
                users.c.owner_id,
                users.c.username,
                nomenclature.c.rating,
                marketplace_rating_aggregates.c.avg_rating,
                marketplace_rating_aggregates.c.reviews_count,
                stock_subquery.c.current_amount,
                total_sold_subquery.c.total_sold,
                cboxes.c.id,  # Нужно для проверки seller_id в sort_priority
            ]
            query = query.group_by(*group_by_fields)

            # --- Сортировка ---
            order = asc if request.sort_order == "asc" else desc

            order_by_clauses = [priority_expr]

            # Затем по выбранному полю
            if request.sort_by == MarketplaceSort.price:
                order_by_clauses.append(order(active_prices_subquery.c.price))
            elif request.sort_by == MarketplaceSort.name:
                order_by_clauses.append(order(nomenclature.c.name))
            elif request.sort_by == MarketplaceSort.rating:
                order_by_clauses.append(
                    order(func.coalesce(marketplace_rating_aggregates.c.avg_rating, 0))
                )
            elif request.sort_by == MarketplaceSort.global_rating:
                order_by_clauses.append(order(func.coalesce(nomenclature.c.rating, 0)))
            elif request.sort_by == MarketplaceSort.total_sold:
                order_by_clauses.append(order(total_sold_subquery.c.total_sold))
            elif request.sort_by == MarketplaceSort.created_at:
                order_by_clauses.append(order(nomenclature.c.created_at))
            elif request.sort_by == MarketplaceSort.updated_at:
                order_by_clauses.append(order(nomenclature.c.updated_at))
            elif request.sort_by == MarketplaceSort.seller:
                order_by_clauses.append(order(cboxes.c.name))
            else:
                order_by_clauses.append(order(total_sold_subquery.c.total_sold))

            query = query.order_by(*order_by_clauses)

            # Пагинация в SQL
            query = query.limit(request.size).offset((request.page - 1) * request.size)

            return query, conditions
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _build_products_query: {elapsed:.4f}s")

    async def _build_products_count_query(
        self,
        request: MarketplaceProductsRequest,
        active_prices_subquery,
        attrs_subquery,
        stock_subquery,
        conditions,
    ):
        """
        Формирует отдельный запрос на подсчёт общего количества товаров.
        Не использует тяжелые агрегаты (array_agg) и window-функции.
        """
        start_time = time.perf_counter()
        try:
            count_query = (
                select(func.count(func.distinct(nomenclature.c.id)))
                .select_from(nomenclature)
                .join(
                    categories, categories.c.id == nomenclature.c.category, isouter=True
                )
                .join(
                    manufacturers,
                    manufacturers.c.id == nomenclature.c.manufacturer,
                    isouter=True,
                )
                .join(
                    active_prices_subquery,
                    active_prices_subquery.c.nomenclature_id == nomenclature.c.id,
                )
                .join(
                    price_types,
                    price_types.c.id == active_prices_subquery.c.price_type,
                )
                .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
                .join(users, users.c.id == cboxes.c.admin, isouter=True)
                .join(
                    marketplace_rating_aggregates,
                    and_(
                        marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                        marketplace_rating_aggregates.c.entity_type == "nomenclature",
                    ),
                    isouter=True,
                )
                .join(
                    stock_subquery,
                    stock_subquery.c.nomenclature_id == nomenclature.c.id,
                    isouter=True,
                )
            )

            if request.nomenclature_attributes:
                count_query = count_query.join(
                    attrs_subquery,
                    attrs_subquery.c.nomenclature_id == nomenclature.c.id,
                    isouter=True,
                )

            count_query = count_query.where(and_(*conditions))
            return count_query
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _build_products_count_query: {elapsed:.4f}s")

    async def _fetch_available_warehouses_map_and_videos(
        self,
        product_ids,
        wb_latest,
        wh_bal,
    ):
        """
        Загружает карты складов и видео по списку товаров.
        Возвращает:
        - available_warehouses_map: nom_id -> список складов с остатками
        - videos_map: nom_id -> список видео
        """
        # Засекаем время загрузки складов и видео
        start_time = time.perf_counter()
        try:
            available_warehouses_map = {}
            videos_map: dict = defaultdict(list)

            if product_ids:
                # Используем wb_latest и wh_bal, которые были определены выше
                warehouses_query = (
                    select(
                        wb_latest.c.nomenclature_id,
                        wh_bal.c.id.label("warehouse_id"),
                        wb_latest.c.organization_id,
                        wh_bal.c.name.label("warehouse_name"),
                        wh_bal.c.address.label("warehouse_address"),
                        wh_bal.c.latitude,
                        wh_bal.c.longitude,
                        wb_latest.c.current_amount,
                    )
                    .select_from(
                        wb_latest.join(
                            wh_bal,
                            and_(
                                wh_bal.c.id == wb_latest.c.warehouse_id,
                                wh_bal.c.is_public.is_(True),
                                wh_bal.c.status.is_(True),
                                wh_bal.c.is_deleted.is_not(True),
                            ),
                        )
                    )
                    .where(
                        and_(
                            wb_latest.c.nomenclature_id.in_(product_ids),
                            wb_latest.c.current_amount > 0,
                        )
                    )
                )
                videos_query = (
                    select(nomenclature_videos)
                    .where(nomenclature_videos.c.nomenclature_id.in_(product_ids))
                    .order_by(
                        nomenclature_videos.c.nomenclature_id.asc(),
                        nomenclature_videos.c.id.asc(),
                    )
                )

                warehouses_rows, videos_rows = await asyncio.gather(
                    database.fetch_all(warehouses_query),
                    database.fetch_all(videos_query),
                )

                for vrow in videos_rows:
                    vd = dict(vrow)
                    nom_id = vd.pop("nomenclature_id")
                    videos_map[nom_id].append(vd)

                for row in warehouses_rows:
                    nom_id = row["nomenclature_id"]
                    if nom_id not in available_warehouses_map:
                        available_warehouses_map[nom_id] = []
                    available_warehouses_map[nom_id].append(
                        {
                            "warehouse_id": row["warehouse_id"],
                            "organization_id": row["organization_id"],
                            "warehouse_name": row["warehouse_name"],
                            "warehouse_address": row["warehouse_address"],
                            "latitude": row["latitude"],
                            "longitude": row["longitude"],
                            "current_amount": row["current_amount"],
                        }
                    )

            return available_warehouses_map, videos_map
        finally:
            elapsed = time.perf_counter() - start_time
            print(
                f"[TIMING] _fetch_available_warehouses_map_and_videos: {elapsed:.4f}s"
            )

    def _build_products_from_rows(
        self,
        products_db,
        request: MarketplaceProductsRequest,
        available_warehouses_map,
        videos_map,
    ) -> List[MarketplaceProduct]:
        """
        Преобразует сырые строки из БД в объекты MarketplaceProduct.
        Здесь выполняется пост-обработка: фото, баркоды, склады, расстояния, админ.
        """
        # Засекаем время пост-обработки товаров
        start_time = time.perf_counter()
        try:
            products: List[MarketplaceProduct] = []
            for index, product in enumerate(products_db):
                product_dict = dict(product)
                product_dict.pop("sort_priority", None)
                product_dict.pop("total_count", None)
                product_dict["listing_pos"] = (
                    (request.page - 1) * request.size + index + 1
                )
                product_dict["listing_page"] = request.page
                product_dict["videos"] = videos_map.get(product_dict["id"], [])

                # Images - преобразуем ID фото в публичные URL
                images = product_dict.get("images")
                # PostgreSQL array_agg возвращает список или None
                if images is not None:
                    # array_agg может вернуть список, даже если он пустой
                    if isinstance(images, list):
                        # Фильтруем None значения и преобразуем ID в URL
                        image_ids = [pid for pid in images if pid is not None]
                        if image_ids:
                            product_dict["images"] = [
                                self.__build_picture_url(picture_id)
                                for picture_id in image_ids
                            ]
                            # Убираем None значения
                            product_dict["images"] = [
                                url for url in product_dict["images"] if url is not None
                            ] or None
                        else:
                            product_dict["images"] = None
                    else:
                        product_dict["images"] = None
                else:
                    product_dict["images"] = None

                # Barcodes
                barcodes = product_dict.get("barcodes")
                product_dict["barcodes"] = (
                    [code for code in barcodes if code]
                    if barcodes and any(barcodes)
                    else None
                )

                # Список складов (только с остатком > 0)
                # Получаем из отдельного запроса, который мы выполнили выше
                wh_list = available_warehouses_map.get(product_dict["id"], [])
                if wh_list:
                    product_dict["available_warehouses"] = sorted(
                        [
                            AvailableWarehouse(
                                **w,
                                distance_to_client=self._count_distance_to_client(
                                    request.lat,
                                    request.lon,
                                    w["latitude"],
                                    w["longitude"],
                                ),
                            )
                            for w in wh_list
                        ],
                        key=lambda x: (
                            x.distance_to_client is None,
                            x.distance_to_client or 0,
                        ),
                    )
                else:
                    product_dict["available_warehouses"] = None

                # Остальные поля
                product_dict["is_ad_pos"] = False
                product_dict["variations"] = []
                product_dict["distance"] = (
                    min(
                        product_dict["available_warehouses"],
                        key=lambda x: x.distance_to_client,
                    ).distance_to_client
                    if product_dict["available_warehouses"]
                    else None
                )
                product_dict["cashbox_id"] = product_dict["cashbox"]
                product_dict["seller_photo"] = self.__transform_photo_route(
                    product_dict["seller_photo"]
                )
                # Формируем отдельный список с главным админом кассы
                recipient_id = product_dict.pop("recipient_id", None)
                admin_username = product_dict.pop("username", None)
                if admin_username:
                    admin_username = admin_username.lstrip("@")
                product_dict["user_admin"] = (
                    [
                        MarketplaceProductUserAdmin(
                            recipient_id=str(recipient_id),
                            username=admin_username,
                        )
                    ]
                    if recipient_id
                    else None
                )

                product_button_text = product_buttons_text.get(product["type"]) or {}
                product_dict["button_text"] = product_button_text.get("name")
                product_dict["button_logic"] = product_button_text.get("logic")

                products.append(MarketplaceProduct(**product_dict))

            return products
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _build_products_from_rows: {elapsed:.4f}s")

    def _deduplicate_products(self, products: List[MarketplaceProduct]):
        """
        Дедуплицирует товары по id.
        Если товар повторяется, выбирает запись с меньшим расстоянием,
        а при равных расстояниях — с меньшей ценой.
        """
        # Засекаем время дедупликации товаров
        start_time = time.perf_counter()
        try:
            # Дедупликация: оставляем только один товар с ближайшей ценой
            # Если товар встречается несколько раз, выбираем тот, у которого цена ближе
            # (приоритет: цена с координатами и меньшим расстоянием)
            seen_products = {}
            for product in products:
                product_id = product.id
                if product_id not in seen_products:
                    seen_products[product_id] = product
                else:
                    # Если товар уже есть, выбираем тот, у которого цена ближе
                    existing = seen_products[product_id]
                    # Приоритет: товар с меньшим расстоянием до склада
                    existing_distance = (
                        existing.distance
                        if existing.distance is not None
                        else float("inf")
                    )
                    current_distance = (
                        product.distance
                        if product.distance is not None
                        else float("inf")
                    )

                    # Если текущий товар ближе, заменяем
                    if current_distance < existing_distance:
                        seen_products[product_id] = product
                    # Если расстояния равны, приоритет у товара с меньшей ценой (более выгодная)
                    elif (
                        current_distance == existing_distance
                        and product.price < existing.price
                    ):
                        seen_products[product_id] = product

            # Преобразуем обратно в список
            return list(seen_products.values())
        finally:
            elapsed = time.perf_counter() - start_time
            print(f"[TIMING] _deduplicate_products: {elapsed:.4f}s")

    async def get_products(
        self,
        request: MarketplaceProductsRequest,
    ) -> MarketplaceProductList:
        """
        Основной метод для получения списка товаров маркетплейса.
        Последовательно:
        1) определяет координаты,
        2) строит подзапрос цены,
        3) готовит подзапросы продаж/атрибутов/остатков,
        4) формирует основной запрос,
        5) применяет приоритизацию и пагинацию,
        6) загружает склады/видео,
        7) пост-обрабатывает результаты,
        8) дедуплицирует и применяет сортировку по расстоянию.
        """
        # 1) Координаты клиента (с геокодингом при необходимости)
        client_lat, client_lon = await self._resolve_client_coordinates(request)

        # 2) Актуальная цена для каждой номенклатуры
        active_prices_subquery = self._build_active_prices_subquery(
            request, client_lat, client_lon
        )

        # 3) Подзапросы: продажи, атрибуты, остатки
        total_sold_subquery = self._build_total_sold_subquery(active_prices_subquery)
        attrs_subquery = self._build_attrs_subquery(request)
        wb_latest, stock_subquery, wh_bal = self._build_stock_subqueries(request)

        # 4) Основной запрос товаров
        query, _conditions = await self._build_products_query(
            request,
            active_prices_subquery,
            total_sold_subquery,
            attrs_subquery,
            stock_subquery,
            wb_latest,
            wh_bal,
            client_lat=client_lat,
            client_lon=client_lon,
            city=request.city,
            seller_id=request.seller_id,
            address=request.address,
        )

        # 4.1) Отдельный запрос на подсчёт общего количества
        count_query = await self._build_products_count_query(
            request,
            active_prices_subquery,
            attrs_subquery,
            stock_subquery,
            _conditions,
        )

        # 5) Основной запрос и count запускаем параллельно
        fetch_start = time.perf_counter()
        products_db, total_count = await asyncio.gather(
            database.fetch_all(query),
            database.fetch_val(count_query),
        )
        total_count = total_count or 0
        elapsed = time.perf_counter() - fetch_start
        print(f"[TIMING] fetch products + count (parallel): {elapsed:.4f}s")

        # 6) Склады и видео для найденных товаров
        product_ids = [row["id"] for row in products_db]
        available_warehouses_map, videos_map = (
            await self._fetch_available_warehouses_map_and_videos(
                product_ids, wb_latest, wh_bal
            )
        )

        # 7) Пост-обработка результатов
        products = self._build_products_from_rows(
            products_db,
            request,
            available_warehouses_map,
            videos_map,
        )

        # 8) Дедупликация
        deduplicated_products = self._deduplicate_products(products)
        sellers = []
        if total_count > 0:
            sellers_query = await self._build_sellers_query(
                request,
                active_prices_subquery,
                attrs_subquery,
                stock_subquery,
                _conditions,
            )
            sellers_rows = await database.fetch_all(sellers_query)
            sellers = [{"id": row["id"], "name": row["name"]} for row in sellers_rows]

        # 9) Сортировка по distance (после расчёта distance_to_client)
        if request.sort_by == MarketplaceSort.distance:
            reverse = request.sort_order == "desc"
            deduplicated_products.sort(
                key=lambda x: x.distance if x.distance is not None else float("inf"),
                reverse=reverse,
            )

        return MarketplaceProductList(
            result=deduplicated_products,
            count=total_count,
            page=request.page,
            size=request.size,
            sellers=sellers or None,
        )

    async def _build_sellers_query(
        self,
        request: MarketplaceProductsRequest,
        active_prices_subquery,
        attrs_subquery,
        stock_subquery,
        conditions,
    ):
        """
        Формирует запрос на получение уникальных продавцов (id, name)
        для товаров, прошедших фильтры.
        """
        query = (
            select(
                cboxes.c.id.label("id"),
                func.coalesce(
                    func.nullif(cboxes.c.seller_name, ""), cboxes.c.name
                ).label("name"),
            )
            .select_from(nomenclature)
            .join(categories, categories.c.id == nomenclature.c.category, isouter=True)
            .join(
                manufacturers,
                manufacturers.c.id == nomenclature.c.manufacturer,
                isouter=True,
            )
            .join(
                active_prices_subquery,
                active_prices_subquery.c.nomenclature_id == nomenclature.c.id,
            )
            .join(price_types, price_types.c.id == active_prices_subquery.c.price_type)
            .join(cboxes, cboxes.c.id == nomenclature.c.cashbox, isouter=True)
            .join(users, users.c.id == cboxes.c.admin, isouter=True)
            .join(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == nomenclature.c.id,
                    marketplace_rating_aggregates.c.entity_type == "nomenclature",
                ),
                isouter=True,
            )
            .join(
                stock_subquery,
                stock_subquery.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )
        )

        if request.nomenclature_attributes:
            query = query.join(
                attrs_subquery,
                attrs_subquery.c.nomenclature_id == nomenclature.c.id,
                isouter=True,
            )

        query = query.where(and_(*conditions))
        query = query.group_by(cboxes.c.id, cboxes.c.seller_name, cboxes.c.name).having(
            cboxes.c.id.is_not(None)
        )

        return query

    async def _fetch_available_warehouses(
        self,
        nomenclature_id: int,
        client_lat: Optional[float] = None,
        client_lon: Optional[float] = None,
    ) -> List[AvailableWarehouse]:
        """
        Получает список публичных, активных и неудалённых складов,
        на которых есть остатки указанной номенклатуры,
        и возвращает их как AvailableWarehouse с расстоянием до клиента.
        """

        # Формируем JSON-объект для каждого склада
        json_obj = func.jsonb_build_object(
            literal_column("'warehouse_id'"),
            warehouses.c.id,
            literal_column("'organization_id'"),
            warehouse_balances_latest.c.organization_id,
            literal_column("'warehouse_name'"),
            warehouses.c.name,
            literal_column("'warehouse_address'"),
            warehouses.c.address,
            literal_column("'latitude'"),
            warehouses.c.latitude,
            literal_column("'longitude'"),
            warehouses.c.longitude,
        )

        # Запрос: склады с остатками по указанной номенклатуре
        query = (
            select(json_obj)
            .select_from(warehouse_balances_latest)
            .join(
                warehouses,
                and_(
                    warehouses.c.id == warehouse_balances_latest.c.warehouse_id,
                    warehouses.c.is_public.is_(True),
                    warehouses.c.status.is_(True),
                    warehouses.c.is_deleted.is_not(True),
                ),
            )
            .where(
                and_(
                    warehouse_balances_latest.c.nomenclature_id == nomenclature_id,
                    # Можно добавить условие на наличие остатка, если нужно:
                    # warehouse_balances_latest.c.current_amount > 0
                )
            )
        )

        rows = await database.fetch_all(query)
        raw_warehouses = []
        for row in rows:
            if row and row[0]:
                # row[0] — это JSON-строка (str), нужно распарсить
                try:
                    wh_dict = json.loads(row[0])
                    raw_warehouses.append(wh_dict)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue  # пропустить некорректные записи

        if not raw_warehouses:
            return []

        result = []
        for w in raw_warehouses:
            result.append(
                AvailableWarehouse(
                    **w,
                    distance_to_client=self._count_distance_to_client(
                        client_lat, client_lon, w["latitude"], w["longitude"]
                    ),
                )
            )

        # Сортируем: сначала склады с известным расстоянием (по возрастанию), потом — без координат
        result.sort(
            key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0)
        )
        return result

    async def log_search(
        self,
        request: MarketplaceProductsRequest,
        result_count: int,
    ) -> None:
        """
        Логирование поискового запроса маркетплейса.

        Логируем ТОЛЬКО если есть текстовый поиск (request.name).
        """

        # Логируем только если есть текст поиска
        if not request.name or not request.name.strip():
            return

        # если телефон передавался, находим или создаем клиента, в ином случае айдишника не будет
        phone = None
        if request.phone:
            phone = BaseMarketplaceService._validate_phone(request.phone)
            client_id = await self._ensure_marketplace_client(phone)
        else:
            client_id = None

        try:
            # Формируем фильтры (исключаем не-фильтры)
            filters_data = request.dict(
                exclude={
                    "name",
                    "phone",
                    "page",
                    "size",
                },
                exclude_none=True,
            )
            await database.execute(
                marketplace_searches.insert().values(
                    phone=phone,
                    query=request.name,
                    filters=filters_data,
                    results_count=result_count,
                    client_id=client_id,
                    created_at=datetime.utcnow(),
                )
            )

        # не останавливаем выполнение
        except Exception as e:
            pass
