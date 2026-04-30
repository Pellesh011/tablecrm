"""Парсер для входящих заказов из CommerceML"""

import logging
from datetime import datetime
from xml.etree import ElementTree as ET

from database.db import (
    contragents,
    database,
    docs_sales,
    docs_sales_goods,
    nomenclature,
    organizations,
    price_types,
    units,
    users_cboxes_relation,
    warehouses,
)
from sqlalchemy import select

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def _get_default_price_type_id(cashbox_id: int):
    """Тип цены по умолчанию для кассы (первый доступный)."""
    q = (
        select(price_types.c.id)
        .where(
            price_types.c.cashbox == cashbox_id,
            price_types.c.is_deleted == False,
        )
        .limit(1)
    )
    r = await database.fetch_one(q)
    return r.id if r else None


async def _get_default_unit_id():
    """Единица по умолчанию (шт или первая)."""
    q = (
        select(units.c.id)
        .where(
            (units.c.convent_national_view == "шт")
            | (units.c.name == "шт")
            | (units.c.symbol_national_view == "шт")
        )
        .limit(1)
    )
    r = await database.fetch_one(q)
    if r:
        return r.id
    r = await database.fetch_one(select(units.c.id).limit(1))
    return r.id if r else None


def find_with_namespace(elem, tag: str):
    """Ищет элемент с namespace или без"""
    result = elem.find(f"{{urn:1C.ru:commerceml_2}}{tag}")
    if result is not None:
        return result
    return elem.find(tag)


def findall_with_namespace(elem, tag: str):
    """Ищет все элементы с namespace или без"""
    results = elem.findall(f"{{urn:1C.ru:commerceml_2}}{tag}")
    if results:
        return results
    return elem.findall(tag)


async def parse_order_xml(xml_content: str, cashbox_id: int):
    """Парсит заказы из CommerceML и сохраняет в БД"""
    try:
        root = ET.fromstring(xml_content)

        # Находим документы продажи (с namespace или без)
        documents = findall_with_namespace(root, "Документ")
        if not documents:
            documents = root.findall(".//Документ") or root.findall(
                ".//{urn:1C.ru:commerceml_2}Документ"
            )

        for doc in documents:
            await process_order(doc, cashbox_id)

        logger.info(f"Обработано {len(documents)} заказов")
    except Exception as e:
        logger.error(f"Ошибка парсинга заказов: {str(e)}", exc_info=True)
        raise


async def process_order(doc: ET.Element, cashbox_id: int):
    """Обрабатывает один заказ из CommerceML"""
    try:
        # Получаем ID документа
        doc_id_elem = find_with_namespace(doc, "Ид")
        if doc_id_elem is None:
            return

        external_id = doc_id_elem.text

        # Проверяем, не обработан ли уже этот заказ
        query = select(docs_sales).where(
            docs_sales.c.order_source == f"commerceml:{external_id}",
            docs_sales.c.cashbox == cashbox_id,
        )
        existing = await database.fetch_one(query)
        if existing:
            logger.debug(f"Заказ {external_id} уже обработан")
            return

        # Получаем номер
        number_elem = find_with_namespace(doc, "Номер")
        number = number_elem.text if number_elem is not None else external_id

        # Получаем дату
        date_elem = find_with_namespace(doc, "Дата")
        if date_elem is not None:
            try:
                date_str = date_elem.text
                # Парсим дату в формате CommerceML (YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS)
                if "T" in date_str:
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                dated = int(dt.timestamp())
            except:
                dated = int(datetime.now().timestamp())
        else:
            dated = int(datetime.now().timestamp())

        # Получаем контрагента
        contragents_elem = find_with_namespace(doc, "Контрагенты")
        contragent_id = None
        if contragents_elem is not None:
            contragent_elem = find_with_namespace(contragents_elem, "Контрагент")
            if contragent_elem is not None:
                contragent_id_elem = find_with_namespace(contragent_elem, "Ид")
                if contragent_id_elem is not None:
                    contragent_external_id = contragent_id_elem.text
                    contragent_id = await get_or_create_contragent(
                        contragent_external_id, contragent_elem, cashbox_id
                    )

        # Получаем организацию (первую доступную для кассы)
        org_query = (
            select(organizations)
            .where(
                organizations.c.cashbox == cashbox_id,
                organizations.c.is_deleted == False,
            )
            .limit(1)
        )
        org = await database.fetch_one(org_query)
        if not org:
            logger.warning(f"Организация не найдена для кассы {cashbox_id}")
            return

        # Получаем склад (первый доступный)
        warehouse_query = (
            select(warehouses)
            .where(
                warehouses.c.cashbox == cashbox_id,
                warehouses.c.is_deleted == False,
            )
            .limit(1)
        )
        warehouse = await database.fetch_one(warehouse_query)
        warehouse_id = warehouse.id if warehouse else None

        # Получаем товары
        goods_elems = findall_with_namespace(doc, "Товар")
        if not goods_elems:
            # Пробуем найти в секции Товары
            goods_section = find_with_namespace(doc, "Товары")
            if goods_section is not None:
                goods_elems = findall_with_namespace(goods_section, "Товар")

        if not goods_elems:
            logger.warning(f"Заказ {external_id} не содержит товаров")
            return

        # Создаем документ продажи
        doc_data = {
            "number": number,
            "dated": dated,
            "operation": "Заказ товара",
            "cashbox": cashbox_id,
            "contragent": contragent_id,
            "organization": org.id,
            "warehouse": warehouse_id,
            "order_source": f"commerceml:{external_id}",
            "status": False,  # Не оплачен
            "is_deleted": False,
        }

        query = docs_sales.insert().values(doc_data).returning(docs_sales.c.id)
        doc_sales_id = await database.execute(query)

        # Добавляем товары
        for good_elem in goods_elems:
            await process_order_good(good_elem, doc_sales_id, cashbox_id)

        logger.info(f"Создан заказ {number} (ID: {doc_sales_id})")
    except Exception as e:
        logger.error(f"Ошибка обработки заказа: {str(e)}", exc_info=True)
        raise


async def process_order_good(good_elem: ET.Element, doc_sales_id: int, cashbox_id: int):
    """Обрабатывает один товар в заказе"""
    try:
        # Получаем ID товара
        good_id_elem = find_with_namespace(good_elem, "Ид")
        if good_id_elem is None:
            return

        external_id = good_id_elem.text

        # Ищем товар в нашей системе
        query = select(nomenclature).where(
            nomenclature.c.external_id == external_id,
            nomenclature.c.cashbox == cashbox_id,
        )
        nom = await database.fetch_one(query)

        if not nom:
            logger.warning(f"Товар {external_id} не найден в системе")
            return

        # Получаем количество
        quantity_elem = find_with_namespace(good_elem, "Количество")
        quantity = float(quantity_elem.text) if quantity_elem is not None else 1.0

        # Получаем цену
        price_elem = find_with_namespace(good_elem, "ЦенаЗаЕдиницу")
        price = float(price_elem.text) if price_elem is not None else 0.0

        # Тип цены и единица: из кассы/номенклатуры или по умолчанию
        price_type_id = await _get_default_price_type_id(cashbox_id)
        unit_id = getattr(nom, "unit", None)
        if unit_id is None:
            unit_id = await _get_default_unit_id()

        # Создаем запись о товаре
        good_data = {
            "docs_sales_id": doc_sales_id,
            "nomenclature": nom.id,
            "quantity": quantity,
            "price": price,
        }
        if price_type_id is not None:
            good_data["price_type"] = price_type_id
        if unit_id is not None:
            good_data["unit"] = unit_id

        query = docs_sales_goods.insert().values(good_data)
        await database.execute(query)
    except Exception as e:
        logger.error(f"Ошибка обработки товара в заказе: {str(e)}", exc_info=True)


async def get_or_create_contragent(
    external_id: str, contragent_elem: ET.Element, cashbox_id: int
) -> int:
    """Получает или создает контрагента"""
    # Ищем по external_id
    query = select(contragents).where(
        contragents.c.external_id == external_id,
        contragents.c.cashbox == cashbox_id,
    )
    existing = await database.fetch_one(query)

    if existing:
        return existing.id

    # Получаем название контрагента
    name_elem = find_with_namespace(contragent_elem, "Наименование")
    name = name_elem.text if name_elem is not None else external_id

    owner_query = (
        select(users_cboxes_relation)
        .where(users_cboxes_relation.c.cashbox_id == cashbox_id)
        .limit(1)
    )
    owner = await database.fetch_one(owner_query)

    if not owner:
        logger.warning(f"Owner not found for cashbox {cashbox_id}")
        return None

    contragent_data = {
        "external_id": external_id,
        "name": name,
        "cashbox": cashbox_id,
        "owner": owner["id"],
        "is_deleted": False,
    }

    query = contragents.insert().values(contragent_data).returning(contragents.c.id)
    contragent_id = await database.execute(query)

    return contragent_id
