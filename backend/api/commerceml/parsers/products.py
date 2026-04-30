"""CommerceML: парсер каталога (товары, описание, тип, фото)."""

import logging
from typing import Optional
from xml.etree import ElementTree as ET

from database.db import (
    categories,
    cboxes,
    database,
    nomenclature,
    pictures,
    units,
    users_cboxes_relation,
)
from sqlalchemy import select

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def find_with_namespace(elem, tag: str):
    """Поиск тега с namespace urn:1C или без."""
    result = elem.find(f"{{urn:1C.ru:commerceml_2}}{tag}")
    if result is not None:
        return result
    return elem.find(tag)


def findall_with_namespace(elem, tag: str):
    """Все теги с namespace или без."""
    results = elem.findall(f"{{urn:1C.ru:commerceml_2}}{tag}")
    if results:
        return results
    return elem.findall(tag)


def _element_text(elem) -> Optional[str]:
    """Текст элемента и дочерних (в т.ч. CDATA)."""
    if elem is None:
        return None
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.extend(ET.itertext(child))
        if child.tail:
            parts.append(child.tail)
    text = "".join(parts).strip() if parts else (elem.text or "").strip()
    return text if text else None


_DESCRIPTION_REQUISITE_NAMES = (
    "Описание",
    "ОписаниеВФорматеHTML",
    "ПолноеОписание",
)


def _parse_description_from_requisites(product: ET.Element) -> Optional[str]:
    """Описание из реквизитов (Описание, ОписаниеВФорматеHTML, ПолноеОписание)."""
    rekv_block = find_with_namespace(product, "ЗначенияРеквизитов")
    if rekv_block is None:
        rekv_block = product.find(".//ЗначенияРеквизитов") or product.find(
            ".//{urn:1C.ru:commerceml_2}ЗначенияРеквизитов"
        )
    if rekv_block is None:
        return None
    for rekv in findall_with_namespace(rekv_block, "ЗначениеРеквизита"):
        name_el = find_with_namespace(rekv, "Наименование")
        val_el = find_with_namespace(rekv, "Значение")
        if name_el is None or val_el is None:
            continue
        name = (name_el.text or "").strip()
        if name not in _DESCRIPTION_REQUISITE_NAMES:
            continue
        val = _element_text(val_el) or (val_el.text or "").strip()
        if val:
            return val
    return None


def _parse_base_unit(product: ET.Element) -> Optional[tuple]:
    """БазоваяЕдиница: (код или None, наименование/символ). Код и Наименование могут быть атрибутами или текст узла."""
    unit_elem = find_with_namespace(product, "БазоваяЕдиница")
    if unit_elem is None:
        unit_elem = product.find(".//БазоваяЕдиница") or product.find(
            ".//{urn:1C.ru:commerceml_2}БазоваяЕдиница"
        )
    if unit_elem is None:
        return None
    # Атрибуты Код, Наименование / НаименованиеПолное (CommerceML 2.0)
    code = unit_elem.get("Код") or unit_elem.get("code")
    name_attr = (
        unit_elem.get("Наименование")
        or unit_elem.get("НаименованиеПолное")
        or unit_elem.get("name")
    )
    text = (unit_elem.text or "").strip()
    name = name_attr or text or "шт"
    return (code, name)


def _parse_nomenclature_type(product: ET.Element) -> Optional[str]:
    """Тип из реквизита ТипНоменклатуры: Услуга → service, иначе product."""
    rekv_block = find_with_namespace(product, "ЗначенияРеквизитов")
    if rekv_block is None:
        rekv_block = product.find(".//ЗначенияРеквизитов") or product.find(
            ".//{urn:1C.ru:commerceml_2}ЗначенияРеквизитов"
        )
    if rekv_block is None:
        return None
    for rekv in findall_with_namespace(rekv_block, "ЗначениеРеквизита"):
        name_el = find_with_namespace(rekv, "Наименование")
        val_el = find_with_namespace(rekv, "Значение")
        if name_el is not None and val_el is not None and val_el.text:
            name = (name_el.text or "").strip()
            val = (val_el.text or "").strip()
            if name == "ТипНоменклатуры":
                if val.lower() in ("услуга", "service"):
                    return "service"
                return "product"
    return None


async def parse_catalog_xml(
    xml_content: str,
    cashbox_id: int,
    session_files: Optional[dict] = None,
) -> int:
    """Каталог → номенклатура, описание, тип, категория, фото. session_files: имя → bytes."""
    logger.info(
        f"commerceml parser Starting catalog parse for cashbox {cashbox_id}, XML size={len(xml_content)}"
    )
    try:
        root = ET.fromstring(xml_content)
        session_files = session_files or {}

        catalog = find_with_namespace(root, "Каталог")
        if catalog is None:
            catalog = root.find(".//Каталог") or root.find(
                ".//{urn:1C.ru:commerceml_2}Каталог"
            )

        if not catalog:
            logger.warning("Каталог не найден в XML")
            return 0

        products = findall_with_namespace(catalog, "Товар")
        if not products:
            products = catalog.findall(".//Товар") or catalog.findall(
                ".//{urn:1C.ru:commerceml_2}Товар"
            )
        logger.info(f"commerceml parser Found {len(products)} products in XML")
        count = 0
        for product in products:
            await process_product(product, cashbox_id, session_files)
            count += 1

        logger.info(f"Обработано {count} товаров")
        return count
    except Exception as e:
        logger.error(f"Ошибка парсинга каталога: {str(e)}", exc_info=True)
        raise


async def process_product(
    product: ET.Element, cashbox_id: int, session_files: Optional[dict] = None
):
    """Один Товар: сохранение в nomenclature, фото из session_files при необходимости."""
    session_files = session_files or {}
    try:
        product_id = find_with_namespace(product, "Ид")
        if product_id is None:
            return

        external_id = product_id.text
        logger.debug(f"commerceml Processing product external_id={external_id}")
        name_elem = find_with_namespace(product, "Наименование")
        name = name_elem.text if name_elem is not None else ""

        if not name:
            return

        # Артикул → поле code
        article_elem = find_with_namespace(product, "Артикул")
        article = (
            (article_elem.text or "").strip() if article_elem is not None else None
        )

        description_elem = find_with_namespace(product, "Описание")
        if description_elem is None:
            description_elem = product.find(".//Описание") or product.find(
                ".//{urn:1C.ru:commerceml_2}Описание"
            )
        description = _element_text(description_elem)
        if not description:
            description = _parse_description_from_requisites(product)

        nom_type = _parse_nomenclature_type(product)
        unit_id = None
        base_unit = _parse_base_unit(product)
        if base_unit:
            unit_id = await get_or_create_unit(base_unit)

        groups_elem = find_with_namespace(product, "Группы")
        category_id = None
        if groups_elem is not None:
            group_id_elem = find_with_namespace(groups_elem, "Ид")
            if group_id_elem is not None:
                category_external_id = group_id_elem.text
                category_id = await get_or_create_category(
                    category_external_id, cashbox_id
                )

        query = select(nomenclature).where(
            nomenclature.c.external_id == external_id,
            nomenclature.c.cashbox == cashbox_id,
        )
        existing = await database.fetch_one(query)

        if existing:
            update_data = {
                "name": name,
                "category": category_id,
            }
            logger.debug(f"commerceml Updating existing product id={existing.id}")
            if article is not None:
                update_data["code"] = article
            if description is not None:
                update_data["description_short"] = description
                update_data["description_long"] = description
            if nom_type is not None:
                update_data["type"] = nom_type
            if unit_id is not None:
                update_data["unit"] = unit_id
            query = (
                nomenclature.update()
                .where(nomenclature.c.id == existing.id)
                .values(update_data)
            )
            await database.execute(query)
            nom_id = existing.id
            owner_relation_id = existing.owner
        else:
            cashbox_query = select(cboxes).where(cboxes.c.id == cashbox_id)
            cashbox = await database.fetch_one(cashbox_query)
            logger.debug("commerceml Creating new product")
            if not cashbox:
                logger.warning(f"Cashbox {cashbox_id} not found")
                return

            owner_query = (
                select(users_cboxes_relation)
                .where(users_cboxes_relation.c.cashbox_id == cashbox_id)
                .limit(1)
            )
            owner = await database.fetch_one(owner_query)

            if not owner:
                logger.warning(f"Owner not found for cashbox {cashbox_id}")
                return

            insert_data = {
                "external_id": external_id,
                "name": name,
                "category": category_id,
                "owner": owner.id,
                "cashbox": cashbox_id,
                "is_deleted": False,
            }
            if article is not None:
                insert_data["code"] = article
            if description is not None:
                insert_data["description_short"] = description
                insert_data["description_long"] = description
            if nom_type is not None:
                insert_data["type"] = nom_type
            if unit_id is not None:
                insert_data["unit"] = unit_id
            query = (
                nomenclature.insert().values(insert_data).returning(nomenclature.c.id)
            )
            nom_id = await database.execute(query)
            owner_relation_id = owner.id

            if not nom_id:
                logger.warning(f"Failed to insert nomenclature {external_id}")
                return

        picture_urls = []
        for el in findall_with_namespace(product, "Картинка"):
            if el.text and el.text.strip():
                picture_urls.append(el.text.strip())
                logger.debug(
                    f"commerceml Found {len(picture_urls)} pictures for product {external_id}"
                )
        if not picture_urls:
            for el in product.findall(".//Картинка") or product.findall(
                ".//{urn:1C.ru:commerceml_2}Картинка"
            ):
                if el.text and el.text.strip():
                    picture_urls.append(el.text.strip())

        for idx, url in enumerate(picture_urls):
            url = url.strip()
            if not url:
                continue
            if url.startswith(("http://", "https://")):
                try:
                    await database.execute(
                        pictures.insert().values(
                            entity="nomenclature",
                            entity_id=nom_id,
                            url=url,
                            is_main=(idx == 0),
                            owner=owner_relation_id,
                            cashbox=cashbox_id,
                            is_deleted=False,
                        )
                    )
                except Exception as e:
                    logger.warning(
                        "Не удалось сохранить картинку (URL) для товара %s: %s",
                        external_id,
                        e,
                    )
                continue
            raw = session_files.get(url)
            if raw is None:
                logger.debug(
                    "CommerceML: нет файла в сессии для товара %s: %s",
                    external_id,
                    url[:50] + "..." if len(url) > 50 else url,
                )
                continue
            if isinstance(raw, bytes):
                file_bytes = raw
            else:
                read_fn = getattr(raw, "read", None)
                file_bytes = read_fn() if callable(read_fn) else raw
            if not isinstance(file_bytes, bytes) or not file_bytes:
                continue
            try:
                from api.pictures.routers import create_picture_from_bytes

                await create_picture_from_bytes(
                    file_bytes,
                    entity="nomenclature",
                    entity_id=nom_id,
                    owner_id=owner_relation_id,
                    cashbox_id=cashbox_id,
                    is_main=(idx == 0),
                )
            except Exception as e:
                logger.warning(
                    "CommerceML: не удалось загрузить картинку для товара %s: %s",
                    external_id,
                    e,
                )

        logger.debug(f"commerceml Обработан товар: {name} (ID: {external_id})")
    except Exception as e:
        logger.error(f"commerceml Ошибка обработки товара: {str(e)}", exc_info=True)


async def get_or_create_unit(code_or_name) -> Optional[int]:
    """Единица измерения по коду (OKEI) или наименованию/символу; создаёт при отсутствии. Без привязки к кассе."""
    if not code_or_name:
        return None
    code_int = None
    logger.debug(f"commerceml Getting/creating unit: {code_or_name}")
    if isinstance(code_or_name, (tuple, list)):
        code_part, name_part = (
            (code_or_name[0], code_or_name[1])
            if len(code_or_name) >= 2
            else (None, code_or_name[0] if code_or_name else None)
        )
        name = (name_part or "").strip() or "шт"
        if code_part is not None:
            try:
                code_int = int(code_part)
            except (TypeError, ValueError):
                pass
    else:
        s = str(code_or_name).strip()
        name = s or "шт"
        try:
            code_int = int(s)
        except ValueError:
            pass

    # Ищем по коду
    if code_int is not None:
        q = select(units).where(units.c.code == code_int)
        row = await database.fetch_one(q)
        if row:
            logger.debug(f"commerceml Found existing unit id={row.id}")
            return row.id
    # Ищем по наименованию/символу
    for col in (
        units.c.name,
        units.c.convent_national_view,
        units.c.symbol_national_view,
        units.c.symbol_international_view,
    ):
        q = select(units).where(col == name)
        row = await database.fetch_one(q)
        if row:
            return row.id
    logger.debug(f"commerceml Creating new unit with name={name}")
    ins = (
        units.insert()
        .values(
            code=code_int,
            name=name,
            convent_national_view=name,
            symbol_national_view=name,
        )
        .returning(units.c.id)
    )
    uid = await database.execute(ins)
    return uid


async def get_or_create_category(external_id: str, cashbox_id: int):
    """Категория по external_id; создаёт при отсутствии."""
    q = select(categories).where(
        categories.c.external_id == external_id,
        categories.c.cashbox == cashbox_id,
        categories.c.is_deleted.is_not(True),
    )
    logger.debug(f"commerceml Getting/creating category: {external_id}")
    row = await database.fetch_one(q)
    if row:
        logger.debug(f"commerceml Found existing category id={row.id}")
        return row.id
    owner_q = (
        select(users_cboxes_relation)
        .where(users_cboxes_relation.c.cashbox_id == cashbox_id)
        .limit(1)
    )
    owner = await database.fetch_one(owner_q)
    if not owner:
        return None
    logger.debug(f"commerceml Creating new category with name={external_id}")
    ins = (
        categories.insert()
        .values(
            external_id=external_id,
            name=external_id,
            cashbox=cashbox_id,
            owner=owner.id,
            status=True,
            is_deleted=False,
        )
        .returning(categories.c.id)
    )
    cat_id = await database.execute(ins)
    return cat_id
