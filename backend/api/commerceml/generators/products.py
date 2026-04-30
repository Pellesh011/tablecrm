"""Генератор каталога товаров для CommerceML"""

import logging
from datetime import datetime
from xml.sax.saxutils import escape

from database.db import database, nomenclature, pictures
from sqlalchemy import and_, select

logger = logging.getLogger(__name__)


async def generate_products_xml(cashbox_id: int) -> str:
    """Генерирует XML каталога товаров в формате CommerceML 2.10"""
    try:
        date_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Получаем товары
        query = select(nomenclature).where(
            and_(
                nomenclature.c.cashbox == cashbox_id,
                nomenclature.c.is_deleted == False,
            )
        )
        products = await database.fetch_all(query)

        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>\n',
            f'<КоммерческаяИнформация ВерсияСхемы="2.10" ДатаФормирования="{date_str}">\n',
            '  <Каталог СодержитТолькоИзменения="false">\n',
            "    <Ид>catalog</Ид>\n",
            "    <Наименование>Каталог товаров</Наименование>\n",
            "    <Товары>\n",
        ]

        # Получаем все фото для товаров одним запросом
        product_ids = [p.id for p in products]
        photos_query = (
            select(
                pictures.c.entity_id,
                pictures.c.url,
                pictures.c.is_main,
            )
            .where(
                and_(
                    pictures.c.entity == "nomenclature",
                    pictures.c.entity_id.in_(product_ids),
                    pictures.c.is_deleted.is_not(True),
                )
            )
            .order_by(
                pictures.c.entity_id, pictures.c.is_main.desc(), pictures.c.id.asc()
            )
        )
        photos_list = await database.fetch_all(photos_query)

        # Группируем фото по товарам
        photos_dict = {}
        from common.utils.url_helper import get_app_url_for_environment

        app_url = get_app_url_for_environment() or "app.tablecrm.com"  # fallback
        app_url = app_url.rstrip("/")
        if not app_url.startswith(("http://", "https://")):
            app_url = f"https://{app_url}"

        def normalize_photo_url(raw_url: str) -> str:
            """Нормализует ссылку на картинку (feed_generator/criterias/filters.py)"""
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

        for photo in photos_list:
            nom_id = photo.entity_id
            if nom_id not in photos_dict:
                photos_dict[nom_id] = []
            # Нормализуем URL как в Tilda
            normalized_url = normalize_photo_url(photo.url)
            if normalized_url:
                photos_dict[nom_id].append(normalized_url)

        for product in products:
            parts.append("      <Товар>\n")
            # Используем external_id если есть, иначе id
            product_id = product.external_id or str(product.id)
            parts.append(f"        <Ид>{escape(product_id)}</Ид>\n")
            parts.append(
                f"        <Наименование>{escape(product.name or '')}</Наименование>\n"
            )

            if product.description_short:
                parts.append(
                    f"        <Описание>{escape(product.description_short)}</Описание>\n"
                )

            # Добавляем изображения
            if product.id in photos_dict:
                for photo_url in photos_dict[product.id]:
                    parts.append(f"        <Картинка>{escape(photo_url)}</Картинка>\n")

            if product.category:
                parts.append("        <Группы>\n")
                parts.append(f"          <Ид>{escape(str(product.category))}</Ид>\n")
                parts.append("        </Группы>\n")

            parts.append("      </Товар>\n")

        parts.extend(
            [
                "    </Товары>\n",
                "  </Каталог>\n",
                "</КоммерческаяИнформация>\n",
            ]
        )

        return "".join(parts)
    except Exception as e:
        logger.error(f"Ошибка генерации каталога: {str(e)}", exc_info=True)
        raise
