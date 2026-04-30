"""
Универсальный генератор XML фидов
Поддерживает как кастомные XML фиды, так и CommerceML формат для Tilda
"""

import json
import logging
import uuid
from datetime import datetime
from xml.sax.saxutils import escape

from api.feeds.feed_generator.criterias.filters import FeedCriteriaFilter
from database.db import database, feeds, warehouse_balances_latest, warehouses
from sqlalchemy import and_, func, select
from starlette.responses import Response

logger = logging.getLogger(__name__)


class FeedGenerator:
    """Универсальный генератор XML фидов"""

    def __init__(self, url_token: str) -> None:
        self.url_token = url_token
        self.feed = None

    async def get_feed(self):
        """Получает данные фида из базы"""
        if self.url_token:
            query = feeds.select().where(feeds.c.url_token == self.url_token)
            self.feed = await database.fetch_one(query)
        return self.feed

    async def generate(self, as_string: bool = False, cashbox_id: int = None):
        """
        Генерирует кастомный XML фид на основе настроек фида (root_tag, item_tag, field_tags).

        Args:
            as_string: Если True, возвращает XML как строку. Если False, возвращает Response объект.
            cashbox_id: ID кассы (если не указан, берется из фида)

        Returns:
            Response объект или строка с XML, или None если фид не найден
        """
        feed = await self.get_feed()
        if not feed:
            return None

        # Используем cashbox_id из параметра или из фида
        feed_cashbox_id = cashbox_id or feed.get("cashbox_id")
        if not feed_cashbox_id:
            # Если cashbox_id не установлен, возвращаем пустой фид
            if as_string:
                return f'<?xml version="1.0" encoding="utf-8"?>\n<{feed["root_tag"]}>\n</{feed["root_tag"]}>'
            else:
                xml_str = f'<?xml version="1.0" encoding="utf-8"?>\n<{feed["root_tag"]}>\n</{feed["root_tag"]}>'
                xml_bytes = xml_str.encode("utf-8")
                response = Response(content=xml_bytes, media_type="application/xml")
                response.headers["Cache-Control"] = "public, max-age=60"
                return response

        logger.info(f"Generating feed for cashbox_id={feed_cashbox_id}")

        criteria_data = self.feed.get("criteria") or {}
        if isinstance(criteria_data, str):
            criteria_data = json.loads(criteria_data)
        filter = FeedCriteriaFilter(criteria_data, feed_cashbox_id)
        balance = await filter.get_warehouse_balance()

        logger.info(f"Found {len(balance) if balance else 0} products in balance")

        root_tag = feed["root_tag"]
        item_tag = feed["item_tag"]
        tags_map = feed["field_tags"]

        parts = [f'<?xml version="1.0" encoding="utf-8"?>\n<{root_tag}>\n']
        for r in balance:
            parts.append(f"  <{item_tag}>\n")
            for xml_tag, field in tags_map.items():
                val = r.get(field)
                if val is None:
                    continue
                elif isinstance(val, list):
                    for v in val:
                        text = escape(str(v))
                        parts.append(f"    <{xml_tag}>{text}</{xml_tag}>\n")

                elif isinstance(val, dict):
                    if field == "params":
                        for k, v in val.items():
                            parts.append(f'    <{xml_tag} name="{k}">{v}</{xml_tag}>\n')

                else:
                    text = escape(str(val))
                    parts.append(f"    <{xml_tag}>{text}</{xml_tag}>\n")
            parts.append(f"  </{item_tag}>\n")
        parts.append(f"</{root_tag}>")
        xml_str = "".join(parts)

        if as_string:
            return xml_str

        xml_bytes = xml_str.encode("utf-8")
        response = Response(content=xml_bytes, media_type="application/xml")
        response.headers["Cache-Control"] = "public, max-age=60"
        return response

    async def generate_catalog(self, cashbox_id: int = None) -> str:
        """
        Генерирует XML каталога товаров в формате CommerceML 2.07 для Tilda.

        Args:
            cashbox_id: ID кассы

        Returns:
            XML строка в формате CommerceML
        """
        feed = await self.get_feed()
        if not feed:
            return None

        feed_cashbox_id = cashbox_id or feed.get("cashbox_id")
        if not feed_cashbox_id:
            return None

        criteria_data = self.feed.get("criteria") or {}
        if isinstance(criteria_data, str):
            criteria_data = json.loads(criteria_data)
        filter = FeedCriteriaFilter(criteria_data, feed_cashbox_id)
        balance = await filter.get_warehouse_balance()

        # Если balance пустой, используем пустой список - Tilda требует валидный XML даже без товаров
        if not balance:
            balance = []

        # Формируем дату в формате CommerceML
        date_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Tilda не поддерживает XML с namespace
        # Убираем все xmlns и используем версию 2.07 (Tilda поддерживает 2.05-2.07)
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>\n',
            f'<КоммерческаяИнформация ВерсияСхемы="2.07" ДатаФормирования="{date_str}">\n',
            '  <Каталог СодержитТолькоИзменения="false">\n',
            f"    <Ид>{feed.get('tilda_catalog_id') or 'tablecrm-catalog'}</Ид>\n",
            "    <Наименование>Каталог товаров</Наименование>\n",
            "    <Классификатор>\n",
            "      <Ид>6627cca9-adcf-5023-8475-75fd58b55252</Ид>\n",
            "      <Наименование>Категории</Наименование>\n",
            "      <Группы>\n",
            "        <Группа>\n",
            "          <Ид>6627cca9-adcf-5023-8475-75fd58b55252</Ид>\n",
            "          <Наименование>Import</Наименование>\n",
            "        </Группа>\n",
            "      </Группы>\n",
            "    </Классификатор>\n",
            "    <Товары>\n",
        ]

        # Отслеживаем дубликаты товаров по Ид
        added_product_ids = set()

        for r in balance:
            # Используем UUID для товаров
            # Генерируем детерминированный UUID на основе ID товара
            raw_id = r.get("id", "")
            if raw_id:
                # Создаем детерминированный UUID на основе ID товара
                namespace_uuid = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                product_uuid = uuid.uuid5(namespace_uuid, f"product-{raw_id}")
                product_id = str(product_uuid)
            else:
                product_id = ""

            # Пропускаем дубликаты
            if product_id in added_product_ids:
                logger.debug(f"Skipping duplicate product: {product_id}")
                continue
            added_product_ids.add(product_id)

            name = escape(str(r.get("name", "")))
            description_short = (
                escape(str(r.get("description", ""))) if r.get("description") else ""
            )
            description_long = (
                escape(str(r.get("description_long", "")))
                if r.get("description_long")
                else ""
            )
            category = escape(str(r.get("category", ""))) if r.get("category") else ""
            seo_title = (
                escape(str(r.get("seo_title", ""))) if r.get("seo_title") else ""
            )
            seo_description = (
                escape(str(r.get("seo_description", "")))
                if r.get("seo_description")
                else ""
            )
            seo_keywords = r.get("seo_keywords") or []
            params = r.get("params") or {}

            # Порядок элементов: Ид, Наименование, Артикул, БазоваяЕдиница, Описание, Группы, Картинки
            parts.append("      <Товар>\n")
            parts.append(f"        <Ид>{product_id}</Ид>\n")
            parts.append(f"        <Наименование>{name}</Наименование>\n")

            # Артикул (обязательное поле для Tilda)
            article = str(r.get("id", product_id))
            parts.append(f"        <Артикул>{escape(article)}</Артикул>\n")

            # БазоваяЕдиница (обязательное поле для CommerceML)
            parts.append('        <БазоваяЕдиница Код="796">шт</БазоваяЕдиница>\n')

            # В Tilda отправляем только длинное описание.
            # Короткое описание сюда не подмешиваем.
            if description_long:
                description_text = description_long
            else:
                description_text = ""
            parts.append(
                f"        <Описание><![CDATA[{description_text}]]></Описание>\n"
            )

            # ID группы должен существовать в Классификаторе
            group_id = "6627cca9-adcf-5023-8475-75fd58b55252"  # UUID группы из успешного импорта
            parts.append("        <Группы>\n")
            parts.append(f"          <Ид>{group_id}</Ид>\n")
            parts.append("        </Группы>\n")

            # Добавляем характеристики в формате CommerceML (ЗначенияСвойств) после Группы
            # Tilda парсит <ЗначенияСвойств> в отдельное поле "Характеристики"
            if params:
                parts.append("        <ЗначенияСвойств>\n")
                for attr_name, attr_value in params.items():
                    parts.append("          <ЗначенияСвойства>\n")
                    # Используем имя атрибута как Ид (Tilda может требовать определенный формат)
                    parts.append(f"            <Ид>{escape(str(attr_name))}</Ид>\n")
                    parts.append(
                        f"            <Значение>{escape(str(attr_value))}</Значение>\n"
                    )
                    parts.append("          </ЗначенияСвойства>\n")
                parts.append("        </ЗначенияСвойств>\n")

            # CommerceML стандарт не имеет полей для SEO
            # SEO данные настраиваются отдельно в админ-панели Tilda

            # Добавляем картинки только если есть реальные изображения.
            # Важно: для совместимости с Tilda используем теги <Картинка> напрямую внутри <Товар>
            # (без обёртки <Картинки>), т.к. некоторые импортеры её не парсят.
            images = r.get("images")
            if images and isinstance(images, list) and len(images) > 0:
                # Отправляем все актуальные фото в стабильном порядке.
                real_images = []
                seen_images = set()
                for img in images:
                    img_str = str(img).strip()
                    if not img_str or "placeholder" in img_str.lower():
                        continue
                    if img_str in seen_images:
                        continue
                    seen_images.add(img_str)
                    real_images.append(img_str)
                if real_images:

                    for img in real_images:
                        # Убираем двойной префикс из URL (работает с любым доменом)
                        img_url = str(img)
                        # Проверяем любой домен tablecrm.com (dev, app, и т.д.)
                        if "/api/v1/https://" in img_url:
                            # Убираем /api/v1/ и оставляем только https://...
                            img_url = img_url.split("/api/v1/", 1)[1]
                        parts.append(
                            f"        <Картинка>{escape(img_url)}</Картинка>\n"
                        )
            # Если изображений нет - не добавляем блок (как было в успешном варианте)

            parts.append("      </Товар>\n")

        parts.extend(
            [
                "    </Товары>\n",
                "  </Каталог>\n",
                "</КоммерческаяИнформация>\n",
            ]
        )

        xml_result = "".join(parts)
        logger.debug(f"Generated catalog XML: {len(xml_result)} chars")
        return xml_result

    async def generate_offers(self, cashbox_id: int = None) -> str:
        """
        Генерирует XML предложений (цены и остатки) в формате CommerceML 2.07 для Tilda.

        Args:
            cashbox_id: ID кассы

        Returns:
            XML строка в формате CommerceML
        """
        feed = await self.get_feed()
        if not feed:
            return None

        feed_cashbox_id = cashbox_id or feed.get("cashbox_id")
        if not feed_cashbox_id:
            return None

        criteria_data = self.feed.get("criteria") or {}
        if isinstance(criteria_data, str):
            criteria_data = json.loads(criteria_data)
        filter = FeedCriteriaFilter(criteria_data, feed_cashbox_id)
        balance = await filter.get_warehouse_balance()

        # Если balance пустой, используем пустой список - Tilda требует валидный XML даже без товаров
        if not balance:
            balance = []

        tech_cards_flag = criteria_data.get("tech_cards")
        if isinstance(tech_cards_flag, str):
            tech_cards_only = tech_cards_flag.strip().lower() in (
                "1",
                "true",
                "yes",
                "y",
            )
        else:
            tech_cards_only = bool(tech_cards_flag)

        date_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Tilda не поддерживает XML с namespace
        # Убираем все xmlns и используем версию 2.07 (Tilda поддерживает 2.05-2.07)
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>\n',
            f'<КоммерческаяИнформация ВерсияСхемы="2.07" ДатаФормирования="{date_str}">\n',
            '  <ПакетПредложений СодержитТолькоИзменения="false">\n',
            "    <Ид>offers</Ид>\n",
            "    <Наименование>Предложения</Наименование>\n",
        ]

        # Объявляем типы цен перед предложениями (обязательно для Tilda)
        # Используем tilda_price_id из настроек фида или дефолтный "base"
        tilda_price_id = feed.get("tilda_price_id") or "base"
        # Название типа цены: если указан UUID, используем "Базовая цена", иначе можно использовать название из настроек
        tilda_price_name = "Базовая цена"

        parts.append("    <ТипыЦен>\n")
        parts.append("      <ТипЦены>\n")
        parts.append(f"        <Ид>{escape(tilda_price_id)}</Ид>\n")
        parts.append(
            f"        <Наименование>{escape(tilda_price_name)}</Наименование>\n"
        )
        parts.append("        <Валюта>RUB</Валюта>\n")
        parts.append("      </ТипЦены>\n")
        parts.append("    </ТипыЦен>\n")

        # Объявляем склады перед предложениями (обязательно для CommerceML)
        # Получаем tilda_warehouse_id для объявления складов
        tilda_warehouse_id_raw = feed.get("tilda_warehouse_id")
        tilda_warehouse_id = None
        if tilda_warehouse_id_raw is not None:
            if isinstance(tilda_warehouse_id_raw, str):
                try:
                    tilda_warehouse_id = json.loads(tilda_warehouse_id_raw)
                except (json.JSONDecodeError, TypeError):
                    tilda_warehouse_id = tilda_warehouse_id_raw
            elif isinstance(tilda_warehouse_id_raw, list):
                tilda_warehouse_id = tilda_warehouse_id_raw

        if tilda_warehouse_id:
            # Нормализуем: если строка, преобразуем в список
            if isinstance(tilda_warehouse_id, str):
                tilda_warehouse_ids = (
                    [tilda_warehouse_id] if tilda_warehouse_id.strip() else []
                )
            elif isinstance(tilda_warehouse_id, list):
                tilda_warehouse_ids = [
                    w for w in tilda_warehouse_id if isinstance(w, str) and w.strip()
                ]
            else:
                tilda_warehouse_ids = []

            # Объявляем склады в блоке <Склады>
            if tilda_warehouse_ids:
                parts.append("    <Склады>\n")
                for tilda_wh_id in tilda_warehouse_ids:
                    parts.append("      <Склад>\n")
                    parts.append(f"        <Ид>{escape(tilda_wh_id)}</Ид>\n")
                    parts.append(
                        f"        <Наименование>Склад {escape(tilda_wh_id)}</Наименование>\n"
                    )
                    parts.append("      </Склад>\n")
                parts.append("    </Склады>\n")

        parts.append("    <Предложения>\n")

        # Получаем остатки по складам для каждого товара
        # Сначала определяем, какие склады использовать
        criteria_raw = feed.get("criteria")
        if isinstance(criteria_raw, str):
            criteria_data = json.loads(criteria_raw)
        elif isinstance(criteria_raw, dict):
            criteria_data = criteria_raw
        else:
            criteria_data = {}
        our_warehouse_ids = criteria_data.get("warehouse_id")  # Наши склады из БД

        # Получаем все склады кассы, если не указаны в criteria
        if not our_warehouse_ids:
            warehouses_query = select(warehouses.c.id).where(
                warehouses.c.cashbox == feed_cashbox_id
            )
            our_warehouse_ids = [
                row.id for row in await database.fetch_all(warehouses_query)
            ]

        # Если склады не найдены, используем пустой список
        if not our_warehouse_ids:
            our_warehouse_ids = []

        # Получаем остатки по складам для всех товаров
        nomenclature_ids = [r.get("id") for r in balance if r.get("id")]
        warehouse_quantities_by_product = (
            {}
        )  # {nomenclature_id: {warehouse_id: quantity}}

        if nomenclature_ids and our_warehouse_ids:
            # Берем последние остатки по складу и товару, а не максимум по истории.
            balances_query = (
                select(
                    warehouse_balances_latest.c.nomenclature_id,
                    warehouse_balances_latest.c.warehouse_id,
                    func.sum(warehouse_balances_latest.c.current_amount).label(
                        "current_amount"
                    ),
                )
                .where(
                    and_(
                        warehouse_balances_latest.c.nomenclature_id.in_(
                            nomenclature_ids
                        ),
                        warehouse_balances_latest.c.warehouse_id.in_(our_warehouse_ids),
                        warehouse_balances_latest.c.cashbox_id == feed_cashbox_id,
                    )
                )
                .group_by(
                    warehouse_balances_latest.c.nomenclature_id,
                    warehouse_balances_latest.c.warehouse_id,
                )
            )
            balances_rows = await database.fetch_all(balances_query)

            for row in balances_rows:
                nom_id = row.nomenclature_id
                wh_id = row.warehouse_id
                qty = row.current_amount or 0

                if nom_id not in warehouse_quantities_by_product:
                    warehouse_quantities_by_product[nom_id] = {}
                warehouse_quantities_by_product[nom_id][wh_id] = qty

        # Отслеживаем дубликаты предложений по Ид
        # Используем set для отслеживания уже добавленных предложений
        added_offer_ids = set()

        for r in balance:
            # Ид должен совпадать с Ид в catalog
            # Используем тот же UUID формат, что и в generate_catalog
            raw_id = r.get("id", "")
            if raw_id:
                # Тот же детерминированный UUID, что в catalog!
                namespace_uuid = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                product_uuid = uuid.uuid5(namespace_uuid, f"product-{raw_id}")
                product_id = str(product_uuid)
            else:
                product_id = ""

            # Пропускаем дубликаты
            if product_id in added_offer_ids:
                logger.debug(f"Skipping duplicate offer: {product_id}")
                continue

            # Получаем цену
            price = float(r.get("price", 0))

            # Пропускаем товары без цены или с нулевой ценой (Tilda их не примет)
            if price <= 0:
                logger.debug(f"Skipping product {product_id}: price is {price}")
                continue

            # Получаем остатки по складам для этого товара
            nom_id = r.get("id")
            product_warehouse_quantities = warehouse_quantities_by_product.get(
                nom_id, {}
            )

            # Если включен режим техкарт, используем рассчитанное количество
            # (не берем остатки из warehouse_balances)
            tech_cards_qty = None
            if tech_cards_only:
                try:
                    tech_cards_qty = int(r.get("current_amount") or 0)
                except (TypeError, ValueError):
                    tech_cards_qty = 0
                if tech_cards_qty < 0:
                    tech_cards_qty = 0

            added_offer_ids.add(product_id)
            parts.append("      <Предложение>\n")
            # Ид должен совпадать с Ид в catalog для того же товара
            parts.append(f"        <Ид>{product_id}</Ид>\n")
            # ИдТипаЦены должен совпадать с Ид в блоке ТипыЦен
            # Используем тот же ID, что объявлен в <ТипыЦен>
            price_type_id = feed.get("tilda_price_id") or "base"

            parts.append("        <Цены>\n")
            parts.append("          <Цена>\n")
            parts.append(
                f"            <ИдТипаЦены>{escape(price_type_id)}</ИдТипаЦены>\n"
            )
            parts.append(f"            <ЦенаЗаЕдиницу>{price:.2f}</ЦенаЗаЕдиницу>\n")

            # Добавляем старую цену (если указан tilda_discount_price_id)
            discount_price_id = feed.get("tilda_discount_price_id")
            if discount_price_id:
                # Получаем цену со скидкой (если есть)
                # Пока используем базовую цену * 0.9 как пример, можно доработать
                old_price = price * 1.1  # Пример: старая цена на 10% выше
                parts.append(f"            <СтараяЦена>{old_price:.2f}</СтараяЦена>\n")

            parts.append("          </Цена>\n")

            # Если есть цена со скидкой, добавляем вторую цену
            if discount_price_id:
                parts.append("          <Цена>\n")
                parts.append(
                    f"            <ИдТипаЦены>{escape(discount_price_id)}</ИдТипаЦены>\n"
                )
                discount_price = price * 0.9  # Пример: цена со скидкой на 10% ниже
                parts.append(
                    f"            <ЦенаЗаЕдиницу>{discount_price:.2f}</ЦенаЗаЕдиницу>\n"
                )
                parts.append("          </Цена>\n")

            parts.append("        </Цены>\n")

            # Обработка складов: если указан tilda_warehouse_id, создаем блоки <Склад>
            tilda_warehouse_id_raw = feed.get("tilda_warehouse_id")

            # Десериализуем из JSON строки если нужно
            tilda_warehouse_id = None
            if tilda_warehouse_id_raw is not None:
                if isinstance(tilda_warehouse_id_raw, str):
                    # Пытаемся десериализовать из JSON
                    try:
                        tilda_warehouse_id = json.loads(tilda_warehouse_id_raw)
                    except (json.JSONDecodeError, TypeError):
                        # Если не JSON, используем как строку
                        tilda_warehouse_id = tilda_warehouse_id_raw
                elif isinstance(tilda_warehouse_id_raw, list):
                    tilda_warehouse_id = tilda_warehouse_id_raw

            if tilda_warehouse_id:
                # Нормализуем: если строка, преобразуем в список
                if isinstance(tilda_warehouse_id, str):
                    tilda_warehouse_ids = (
                        [tilda_warehouse_id] if tilda_warehouse_id.strip() else []
                    )
                elif isinstance(tilda_warehouse_id, list):
                    tilda_warehouse_ids = [
                        w
                        for w in tilda_warehouse_id
                        if isinstance(w, str) and w.strip()
                    ]
                else:
                    tilda_warehouse_ids = []

                # Маппим наши склады на склады Tilda
                if tech_cards_only and tilda_warehouse_ids:
                    # В режиме техкарт берем рассчитанное количество и кладем на первый склад Tilda
                    tilda_wh_id = tilda_warehouse_ids[0]
                    parts.append("        <Склад>\n")
                    parts.append(f"          <Ид>{escape(tilda_wh_id)}</Ид>\n")
                    parts.append(
                        f"          <Количество>{tech_cards_qty}</Количество>\n"
                    )
                    parts.append("        </Склад>\n")
                    parts.append(f"        <Количество>{tech_cards_qty}</Количество>\n")
                elif tilda_warehouse_ids and our_warehouse_ids:
                    # Суммируем остатки со всех складов для общего количества
                    warehouse_quantities = []  # Сохраняем количества для каждого склада

                    # Маппинг по индексу: первый наш склад -> первый склад Tilda и т.д.
                    for idx, tilda_wh_id in enumerate(tilda_warehouse_ids):
                        # Берем соответствующий наш склад по индексу
                        if idx < len(our_warehouse_ids):
                            our_wh_id = our_warehouse_ids[idx]
                            # Получаем количество на этом складе
                            quantity = product_warehouse_quantities.get(our_wh_id, 0)
                            display_quantity = quantity if quantity > 0 else 0
                            warehouse_quantities.append(display_quantity)

                            parts.append("        <Склад>\n")
                            parts.append(f"          <Ид>{escape(tilda_wh_id)}</Ид>\n")
                            parts.append(
                                f"          <Количество>{display_quantity}</Количество>\n"
                            )
                            parts.append("        </Склад>\n")
                        # Если наш склад не найден для этого индекса, пропускаем склад Tilda
                        # (не добавляем фиктивные остатки в продакшене)

                    # Добавляем общее количество (сумма всех складов) для отображения в Tilda
                    total_quantity = (
                        sum(warehouse_quantities) if warehouse_quantities else 0
                    )
                    parts.append(f"        <Количество>{total_quantity}</Количество>\n")
                elif tilda_warehouse_ids:
                    # Если наши склады не указаны, но есть склады Tilda
                    # Если нет наших складов для маппинга, не придумываем остатки.
                    total_quantity = 0
                    for tilda_wh_id in tilda_warehouse_ids:
                        parts.append("        <Склад>\n")
                        parts.append(f"          <Ид>{escape(tilda_wh_id)}</Ид>\n")
                        parts.append("          <Количество>0</Количество>\n")
                        parts.append("        </Склад>\n")

                    # Добавляем общее количество (сумма всех складов)
                    parts.append(f"        <Количество>{total_quantity}</Количество>\n")
                else:
                    # Если массив пустой, используем старый формат
                    # Суммируем все остатки из всех складов для этого товара
                    # product_warehouse_quantities это {warehouse_id: quantity}
                    if tech_cards_only:
                        parts.append(
                            f"        <Количество>{tech_cards_qty}</Количество>\n"
                        )
                    else:
                        total_quantity = (
                            sum(product_warehouse_quantities.values())
                            if isinstance(product_warehouse_quantities, dict)
                            else 0
                        )
                        parts.append(
                            f"        <Количество>{total_quantity}</Количество>\n"
                        )
            else:
                # Если tilda_warehouse_id не указан, используем старый формат
                # Суммируем остатки со всех складов для этого товара
                # product_warehouse_quantities это {warehouse_id: quantity}
                if tech_cards_only:
                    parts.append(f"        <Количество>{tech_cards_qty}</Количество>\n")
                else:
                    total_quantity = (
                        sum(product_warehouse_quantities.values())
                        if isinstance(product_warehouse_quantities, dict)
                        else 0
                    )
                    parts.append(f"        <Количество>{total_quantity}</Количество>\n")

            parts.append("      </Предложение>\n")

        parts.extend(
            [
                "    </Предложения>\n",
                "  </ПакетПредложений>\n",
                "</КоммерческаяИнформация>\n",
            ]
        )

        xml_result = "".join(parts)
        logger.debug(f"Generated offers XML: {len(xml_result)} chars")
        return xml_result
