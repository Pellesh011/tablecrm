import math
import re
import time
from collections import defaultdict
from typing import Dict, List, Optional

import asyncpg
import phonenumbers
from api.marketplace.schemas import BaseMarketplaceUtm
from api.marketplace.service.products_list_service.schemas import AvailableWarehouse
from api.nomenclature.video.models import nomenclature_videos
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import (
    contragents,
    database,
    marketplace_clients_list,
    marketplace_utm_tags,
    nomenclature,
    warehouse_balances_latest,
    warehouses,
)
from fastapi import HTTPException
from phonenumbers import NumberParseException
from sqlalchemy import and_, select, update
from sqlalchemy.dialects.postgresql import insert


class BaseMarketplaceService:
    def __init__(self):
        self._rabbitmq: Optional[IRabbitMessaging] = None
        self._entity_types_to_tables = {
            "nomenclature": nomenclature,
            "warehouses": warehouses,
        }

    @staticmethod
    async def _get_contragent_id_by_phone(phone: str) -> int:
        try:
            contragent_query = select(contragents.c.id).where(
                contragents.c.phone == BaseMarketplaceService._validate_phone(phone)
            )
            row = await database.fetch_one(contragent_query)
            return row.id
        except AttributeError:
            raise HTTPException(
                status_code=404,
                detail="Контрагент с таким номером телефона не найден",
            )

    @staticmethod
    async def _get_or_create_contragent_id(phone: str, cashbox_id: int) -> int:
        if not phone:
            raise HTTPException(status_code=422, detail="Не указан номер телефона")

        q = select(contragents.c.id).where(
            and_(
                contragents.c.phone == BaseMarketplaceService._validate_phone(phone),
                contragents.c.cashbox == cashbox_id,
                contragents.c.is_deleted.is_not(True),
            )
        )
        row = await database.fetch_one(q)
        if row:
            return row.id

        now_ts = int(time.time())
        ins = (
            contragents.insert()
            .values(
                phone=BaseMarketplaceService._validate_phone(phone),
                name=BaseMarketplaceService._validate_phone(phone),
                cashbox=cashbox_id,
                is_deleted=False,
                is_phone_formatted=False,
                created_at=now_ts,
                updated_at=now_ts,
            )
            .returning(contragents.c.id)
        )

        created = await database.fetch_one(ins)
        if not created:
            raise HTTPException(
                status_code=500,
                detail="Не удалось создать контрагента для оформления заказа",
            )
        return created.id

    @staticmethod
    async def _validate_contragent(contragent_phone: str, nomenclature_id: int) -> None:
        try:
            contragent_query = select(contragents.c.cashbox).where(
                contragents.c.phone
                == BaseMarketplaceService._validate_phone(contragent_phone)
            )
            nomenclature_query = select(nomenclature.c.cashbox).where(
                nomenclature.c.id == nomenclature_id
            )
            if not (
                (await database.fetch_one(contragent_query)).cashbox
                == (await database.fetch_one(nomenclature_query)).cashbox
            ):
                raise HTTPException(
                    status_code=422, detail="Контрагент не принадлежит этому кешбоксу"
                )
        except AttributeError:
            raise HTTPException(
                status_code=404,
                detail="Контрагент или номенклатура с таким номером телефона не найден",
            )

    @staticmethod
    async def _add_utm(entity_id: int, utm: BaseMarketplaceUtm) -> int:
        query = marketplace_utm_tags.insert().values(
            entity_id=entity_id,
            entity_type=utm.entity_type.value,
            **utm.dict(exclude={"entity_type"}),
        )
        res = await database.execute(query)
        return res

    @staticmethod
    def _count_distance_to_client(
        client_lat: Optional[float],
        client_long: Optional[float],
        warehouse_lat: Optional[float],
        warehouse_long: Optional[float],
    ) -> Optional[float]:
        if not all([client_lat, client_long, warehouse_lat, warehouse_long]):
            return None

        R = 6371.0  # радиус Земли в километрах

        lat1_rad = math.radians(client_lat)
        lon1_rad = math.radians(client_long)
        lat2_rad = math.radians(warehouse_lat)
        lon2_rad = math.radians(warehouse_long)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = R * c
        return distance  # в километрах

    async def fetch_videos_map(nomenclature_ids: List[int]) -> Dict[int, List[dict]]:
        """
        Батч-загрузка видео для списка номенклатур (один SQL запрос).
        Возвращает: {nomenclature_id: [{"id": ..., "url": ..., ...}, ...]}
        """
        if not nomenclature_ids:
            return defaultdict(list)

        rows = await database.fetch_all(
            select(nomenclature_videos)
            .where(nomenclature_videos.c.nomenclature_id.in_(nomenclature_ids))
            .order_by(
                nomenclature_videos.c.nomenclature_id.asc(),
                nomenclature_videos.c.id.asc(),
            )
        )

        result: Dict[int, List[dict]] = defaultdict(list)
        for row in rows:
            vd = dict(row)
            nom_id = vd.pop("nomenclature_id")
            result[nom_id].append(vd)

        return result

    @staticmethod
    def _validate_phone(phone: str) -> str:
        """Валидация и нормализация номера телефона к формату E.164 (+7XXXXXXXXXX)

        Удаляет все символы кроме цифр и +, проверяет валидность через phonenumbers,
        нормализует к единому стандарту E.164 для предотвращения дублей.
        """
        if not phone:
            raise HTTPException(status_code=422, detail="Не указан номер телефона")

        # Удаляем все нецифровые символы кроме +
        phone_clean = re.sub(r"[^\d+]", "", phone.strip())

        # Проверяем, что номер не является мусором (только цифры и +)
        # Минимум 10 цифр (для мобильных номеров), максимум 15 (международный стандарт)
        if not re.match(r"^\+?\d{10,15}$", phone_clean):
            # Логируем для диагностики
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f"Invalid phone number rejected: original='{phone}', cleaned='{phone_clean}', length={len(phone_clean)}"
            )
            raise HTTPException(
                status_code=422,
                detail="Некорректный формат номера телефона. Номер должен содержать только цифры и может начинаться с +",
            )

        # Нормализуем и валидируем через phonenumbers
        try:
            # Если номер не начинается с +, добавляем +7 для российских номеров
            if not phone_clean.startswith("+"):
                if phone_clean.startswith("8"):
                    # Номер начинается с 8: заменяем на +7
                    phone_clean = "+7" + phone_clean[1:]
                elif phone_clean.startswith("7"):
                    # Номер начинается с 7: добавляем +
                    phone_clean = "+" + phone_clean
                else:
                    # 10 цифр без кода страны - считаем российским
                    phone_clean = "+7" + phone_clean

            # Пытаемся распарсить с приоритетом на RU (российские номера)
            try:
                parsed_number = phonenumbers.parse(phone_clean, "RU")
            except NumberParseException:
                # Если не получилось с RU, пробуем автоопределение
                parsed_number = phonenumbers.parse(phone_clean, None)

            # Проверяем валидность номера
            if not phonenumbers.is_valid_number(parsed_number):
                raise HTTPException(
                    status_code=422,
                    detail="Некорректный номер телефона",
                )

            # Форматируем в E164 формат (стандарт: +7XXXXXXXXXX)
            # Это гарантирует единый формат для всех номеров и предотвращает дубли
            normalized_phone = phonenumbers.format_number(
                parsed_number, phonenumbers.PhoneNumberFormat.E164
            )
            return normalized_phone
        except NumberParseException as e:
            raise HTTPException(
                status_code=422,
                detail=f"Некорректный формат номера телефона: {str(e)}",
            )

    @staticmethod
    async def _ensure_marketplace_client(
        phone: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        utm_source: Optional[str] = None,
        utm_medium: Optional[str] = None,
        utm_campaign: Optional[str] = None,
        utm_term: Optional[List[str]] = None,
        ref_user: Optional[str] = None,
    ) -> int:
        """Создает или обновляет клиента marketplace с валидацией телефона и сохранением имен и UTM меток"""
        # Валидируем и нормализуем телефон
        # Это выбросит HTTPException 422 для невалидных номеров (например, "pppp" -> пустая строка -> ошибка)
        normalized_phone = BaseMarketplaceService._validate_phone(phone)

        # Проверяем, существует ли клиент
        existing_client = await database.fetch_one(
            select(
                marketplace_clients_list.c.id,
                marketplace_clients_list.c.first_name,
                marketplace_clients_list.c.last_name,
                marketplace_clients_list.c.utm_source,
                marketplace_clients_list.c.utm_medium,
                marketplace_clients_list.c.utm_campaign,
                marketplace_clients_list.c.utm_term,
                marketplace_clients_list.c.ref_user,
                marketplace_clients_list.c.phone,
            ).where(marketplace_clients_list.c.phone == normalized_phone)
        )

        # Дополнительная проверка: если клиент найден, убеждаемся, что его номер валидный
        # (защита от случаев, когда невалидный номер был создан до добавления валидации)
        if existing_client:
            try:
                # Проверяем валидность найденного номера
                BaseMarketplaceService._validate_phone(existing_client.phone)
            except HTTPException:
                # Если номер в БД невалидный, выбрасываем ошибку
                raise HTTPException(
                    status_code=422,
                    detail="Найден клиент с невалидным номером телефона в базе данных. Пожалуйста, свяжитесь с администратором.",
                )

        if existing_client:
            client_id = existing_client.id
            # Обновляем имена и UTM метки, если они переданы и еще не заполнены
            update_values = {}
            if first_name and not existing_client.first_name:
                update_values["first_name"] = first_name
            if last_name and not existing_client.last_name:
                update_values["last_name"] = last_name
            # UTM метки и ref_user сохраняем только при первом создании (если еще не заполнены)
            if utm_source and not existing_client.utm_source:
                update_values["utm_source"] = utm_source
            if utm_medium and not existing_client.utm_medium:
                update_values["utm_medium"] = utm_medium
            if utm_campaign and not existing_client.utm_campaign:
                update_values["utm_campaign"] = utm_campaign
            if utm_term and not existing_client.utm_term:
                update_values["utm_term"] = utm_term
            if ref_user and not existing_client.ref_user:
                update_values["ref_user"] = ref_user

            if update_values:
                await database.execute(
                    update(marketplace_clients_list)
                    .where(marketplace_clients_list.c.id == client_id)
                    .values(**update_values)
                )

            return client_id

        # Создаем нового клиента с UTM метками и ref_user
        # Используем ON CONFLICT DO NOTHING для защиты от дублей при параллельных запросах
        stmt = (
            insert(marketplace_clients_list)
            .values(
                phone=normalized_phone,
                first_name=first_name,
                last_name=last_name,
                utm_source=utm_source,
                utm_medium=utm_medium,
                utm_campaign=utm_campaign,
                utm_term=utm_term,
                ref_user=ref_user,
            )
            .on_conflict_do_nothing(index_elements=["phone"])
            .returning(marketplace_clients_list.c.id)
        )
        try:
            result = await database.fetch_one(stmt)
            if result:
                # Клиент успешно создан
                return result.id

            # Если результат None (ON CONFLICT DO NOTHING сработал),
            # получаем существующего клиента и обновляем его поля, если нужно
            existing_client = await database.fetch_one(
                select(
                    marketplace_clients_list.c.id,
                    marketplace_clients_list.c.first_name,
                    marketplace_clients_list.c.last_name,
                    marketplace_clients_list.c.utm_source,
                    marketplace_clients_list.c.utm_medium,
                    marketplace_clients_list.c.utm_campaign,
                    marketplace_clients_list.c.utm_term,
                    marketplace_clients_list.c.ref_user,
                ).where(marketplace_clients_list.c.phone == normalized_phone)
            )
            if existing_client:
                # Обновляем поля, если они переданы и еще не заполнены
                update_values = {}
                if first_name and not existing_client.first_name:
                    update_values["first_name"] = first_name
                if last_name and not existing_client.last_name:
                    update_values["last_name"] = last_name
                if utm_source and not existing_client.utm_source:
                    update_values["utm_source"] = utm_source
                if utm_medium and not existing_client.utm_medium:
                    update_values["utm_medium"] = utm_medium
                if utm_campaign and not existing_client.utm_campaign:
                    update_values["utm_campaign"] = utm_campaign
                if utm_term and not existing_client.utm_term:
                    update_values["utm_term"] = utm_term
                if ref_user and not existing_client.ref_user:
                    update_values["ref_user"] = ref_user

                if update_values:
                    await database.execute(
                        update(marketplace_clients_list)
                        .where(marketplace_clients_list.c.id == existing_client.id)
                        .values(**update_values)
                    )

                return existing_client.id

            raise HTTPException(
                status_code=500, detail="Не удалось создать клиента marketplace"
            )
        except asyncpg.exceptions.UniqueViolationError:
            # Дополнительная защита от race condition (fallback)
            # Если все же произошел конфликт, получаем существующего клиента
            existing_client = await database.fetch_one(
                select(marketplace_clients_list.c.id).where(
                    marketplace_clients_list.c.phone == normalized_phone
                )
            )
            if existing_client:
                return existing_client.id
            raise HTTPException(
                status_code=500,
                detail="Не удалось создать клиента marketplace: конфликт уникальности",
            )

    @staticmethod
    async def _fetch_available_warehouses(
        nomenclature_id: int,
        client_lat: Optional[float] = None,
        client_lon: Optional[float] = None,
        limit: int = 50,
    ) -> List[AvailableWarehouse]:

        wb_ranked = (
            select(
                warehouse_balances_latest.c.organization_id.label("organization_id"),
                warehouse_balances_latest.c.warehouse_id.label("warehouse_id"),
                warehouse_balances_latest.c.nomenclature_id.label("nomenclature_id"),
                warehouse_balances_latest.c.current_amount.label("current_amount"),
                warehouses.c.name.label("warehouse_name"),
                warehouses.c.address.label("warehouse_address"),
                warehouses.c.latitude.label("latitude"),
                warehouses.c.longitude.label("longitude"),
            )
            .select_from(
                warehouse_balances_latest.join(
                    warehouses,
                    and_(
                        warehouses.c.id == warehouse_balances_latest.c.warehouse_id,
                        warehouses.c.is_public.is_(True),
                        warehouses.c.status.is_(True),
                        warehouses.c.is_deleted.is_not(True),
                    ),
                )
            )
            .where(warehouse_balances_latest.c.nomenclature_id == nomenclature_id)
            .subquery()
        )

        query = (
            select(
                wb_ranked.c.warehouse_id,
                wb_ranked.c.organization_id,
                wb_ranked.c.current_amount,
                wb_ranked.c.warehouse_name,
                wb_ranked.c.warehouse_address,
                wb_ranked.c.latitude,
                wb_ranked.c.longitude,
            )
            .where(
                and_(
                    wb_ranked.c.current_amount > 0,
                )
            )
            .limit(limit)
        )

        rows = await database.fetch_all(query)
        if not rows:
            return []

        result: List[AvailableWarehouse] = []
        for r in rows:
            d = dict(r)
            result.append(
                AvailableWarehouse(
                    warehouse_id=d["warehouse_id"],
                    organization_id=d["organization_id"],
                    warehouse_name=d.get("warehouse_name"),
                    warehouse_address=d.get("warehouse_address"),
                    latitude=d.get("latitude"),
                    longitude=d.get("longitude"),
                    current_amount=d.get("current_amount"),
                    distance_to_client=BaseMarketplaceService._count_distance_to_client(
                        client_lat, client_lon, d.get("latitude"), d.get("longitude")
                    ),
                )
            )

        # Сортировка:
        # - если есть координаты клиента: сначала по расстоянию
        # - иначе: по остатку (DESC) как более практичный дефолт
        if client_lat is not None and client_lon is not None:
            result.sort(
                key=lambda x: (x.distance_to_client is None, x.distance_to_client or 0)
            )
        else:
            result.sort(key=lambda x: -(x.current_amount or 0))

        return result

    @staticmethod
    async def _get_latest_organization_id_for_balance(
        warehouse_id: int, nomenclature_id: int
    ) -> int:
        """Найти organization_id по warehouse+номенклатура (по последнему балансу)."""
        q = (
            select(warehouse_balances_latest.c.organization_id)
            .where(
                and_(
                    warehouse_balances_latest.c.warehouse_id == warehouse_id,
                    warehouse_balances_latest.c.nomenclature_id == nomenclature_id,
                )
            )
            .limit(1)
        )
        row = await database.fetch_one(q)
        if not row:
            raise HTTPException(
                status_code=404,
                detail="Не удалось определить организацию для выбранного склада/товара",
            )
        return row.organization_id
