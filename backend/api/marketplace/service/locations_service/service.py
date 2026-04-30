from api.marketplace.service.base_marketplace_service import BaseMarketplaceService
from api.marketplace.service.locations_service.schemas import (
    LocationsListRequest,
    LocationsListResponse,
)
from common.geocoders.instance import geocoder
from database.db import database, marketplace_rating_aggregates, warehouses
from sqlalchemy import and_, func, literal_column, select


class MarketplaceLocationsService(BaseMarketplaceService):
    async def get_locations(
        self, request: LocationsListRequest
    ) -> LocationsListResponse:
        lat = request.lat
        lon = request.lon
        radius = request.radius
        address = request.address
        city = request.city
        page = request.page
        size = request.size

        # Если переданы address или city, но нет координат - геокодируем адрес
        if (address or city) and (lat is None or lon is None):
            search_address = address or city
            if search_address:
                structured_geo = await geocoder.validate_address(
                    search_address, limit=1
                )
                if (
                    structured_geo
                    and structured_geo.latitude
                    and structured_geo.longitude
                ):
                    # Используем координаты из результата геокодирования
                    lat = structured_geo.latitude
                    lon = structured_geo.longitude
                    # Если радиус не указан, используем значение по умолчанию
                    if radius is None:
                        radius = 20  # 20 км по умолчанию

        offset = (page - 1) * size

        # Основной запрос для получения информации о складах
        # Показываем все публичные склады (независимо от остатков товаров)
        query = select(
            warehouses.c.id,
            warehouses.c.name,
            warehouses.c.address,
            warehouses.c.latitude,
            warehouses.c.longitude,
            warehouses.c.description,
            # Join с рейтингами для получения avg_rating и reviews_count
            marketplace_rating_aggregates.c.avg_rating,
            marketplace_rating_aggregates.c.reviews_count,
        ).select_from(
            warehouses.outerjoin(
                marketplace_rating_aggregates,
                and_(
                    marketplace_rating_aggregates.c.entity_id == warehouses.c.id,
                    marketplace_rating_aggregates.c.entity_type == "warehouse",
                ),
            )
        )

        # Условия для фильтрации складов
        # ВАЖНО: склады должны быть публичными и активными
        conditions = [
            warehouses.c.is_public.is_(True),
            warehouses.c.status.is_(True),
            warehouses.c.is_deleted.is_not(True),
            warehouses.c.address.is_not(None),  # Адрес обязателен
        ]

        # Вычисляем расстояние для всех складов (если есть координаты)
        distance_expr = None
        if lat is not None and lon is not None:
            # Используем формулу Haversine для PostgreSQL
            # earth_radius = 6371 (км)
            distance_expr = (
                func.acos(
                    func.cos(func.radians(lat))
                    * func.cos(func.radians(warehouses.c.latitude))
                    * func.cos(func.radians(warehouses.c.longitude) - func.radians(lon))
                    + func.sin(func.radians(lat))
                    * func.sin(func.radians(warehouses.c.latitude))
                )
                * 6371
            )

        # Фильтрация по местоположению
        if city:
            # Если передан город - всегда ищем по адресу склада
            city_lower = city.lower().strip()

            # Маппинг английских названий городов на русские
            city_mapping = {
                "moscow": "москва",
                "saint petersburg": "санкт-петербург",
                "spb": "санкт-петербург",
                "kazan": "казань",
                "ekaterinburg": "екатеринбург",
                "novosibirsk": "новосибирск",
                "nizhny novgorod": "нижний новгород",
                "samara": "самара",
                "omsk": "омск",
                "rostov-on-don": "ростов-на-дону",
                "chelyabinsk": "челябинск",
                "ufa": "уфа",
                "volgograd": "волгоград",
                "perm": "пермь",
                "krasnoyarsk": "красноярск",
            }

            # Если передан английский вариант - используем русский
            city_ru = city_mapping.get(city_lower, city_lower)

            # Ищем склады, у которых адрес содержит название города
            # Учитываем разные варианты написания: "Москва", "г. Москва", "Москва, ул. ..." и т.д.
            conditions.append(func.lower(warehouses.c.address).ilike(f"%{city_ru}%"))
        elif lat is not None and lon is not None and radius is not None:
            # Если нет города, но есть координаты - используем радиус
            conditions.append(distance_expr <= radius)

        # Добавляем вычисляемое поле расстояния для сортировки и отображения
        if distance_expr is not None:
            query = query.add_columns(distance_expr.label("distance"))
            # Сортировка по расстоянию (ближайшие первыми), если есть координаты
            if city:
                # Если есть город - сортируем сначала по наличию адреса с городом, потом по расстоянию
                query = query.order_by(distance_expr)
            else:
                query = query.order_by(distance_expr)
        else:
            # Если нет координат, сортируем по ID
            query = query.order_by(warehouses.c.id)
            # Добавляем NULL для distance
            query = query.add_columns(literal_column("NULL").label("distance"))

        # Строим запрос с условиями
        query = query.where(and_(*conditions))

        # Запрос для подсчета общего количества записей с теми же условиями
        count_query = (
            select(func.count(warehouses.c.id))
            .select_from(
                warehouses.outerjoin(
                    marketplace_rating_aggregates,
                    and_(
                        marketplace_rating_aggregates.c.entity_id == warehouses.c.id,
                        marketplace_rating_aggregates.c.entity_type == "warehouse",
                    ),
                )
            )
            .where(and_(*conditions))
        )

        total_count = await database.fetch_val(count_query)

        # Применяем пагинацию
        query = query.limit(size).offset(offset)
        locations_db = await database.fetch_all(query)

        # Обрабатываем результаты
        locations = []
        for location in locations_db:
            loc_dict = dict(location)

            # Преобразуем distance в float если оно не None
            if "distance" in loc_dict and loc_dict["distance"] is not None:
                try:
                    loc_dict["distance"] = float(loc_dict["distance"])
                except (TypeError, ValueError):
                    loc_dict["distance"] = None
            elif lat is not None and lon is not None:
                loc_dict["distance"] = self._count_distance_to_client(
                    lat, lon, loc_dict["latitude"], loc_dict["longitude"]
                )
            locations.append(loc_dict)

        return LocationsListResponse(
            **{"locations": locations, "count": total_count, "page": page, "size": size}
        )
