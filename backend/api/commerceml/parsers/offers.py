"""CommerceML: парсер предложений (цены, остатки → документ прихода)."""

import logging
import re
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET

from database.db import (
    OperationType,
    database,
    docs_warehouse,
    docs_warehouse_goods,
    nomenclature,
    organizations,
    price_types,
    prices,
    units,
    users_cboxes_relation,
    warehouse_balances_latest,
    warehouse_register_movement,
    warehouses,
)
from functions.warehouse_events import publish_balance_recalc_batch
from sqlalchemy import select
from sqlalchemy.sql import func

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Тип цены для импорта: chatting (маркетплейс), иначе Основная
_PRICE_TYPE_CHATTING = "chatting"
_PRICE_TYPE_FALLBACK = "Основная"


async def _owner_id_for_cashbox(cashbox_id: int) -> Optional[int]:
    """relation_tg_cashboxes.id для кассы: связь → номенклатура → тип цены."""
    r = await database.fetch_one(
        select(users_cboxes_relation.c.id)
        .where(users_cboxes_relation.c.cashbox_id == cashbox_id)
        .limit(1)
    )
    if r:
        return r.id
    r = await database.fetch_one(
        select(nomenclature.c.owner)
        .where(
            nomenclature.c.cashbox == cashbox_id,
            nomenclature.c.is_deleted.is_not(True),
        )
        .limit(1)
    )
    if r and getattr(r, "owner", None):
        return r.owner
    r = await database.fetch_one(
        select(price_types.c.owner)
        .where(
            price_types.c.cashbox == cashbox_id,
            price_types.c.is_deleted.is_not(True),
        )
        .limit(1)
    )
    return getattr(r, "owner", None) if r else None


def _parse_decimal(text: str):
    """Число из строки: запятая → точка, убрать пробелы и неразрывные пробелы."""
    if not text:
        raise ValueError("empty")
    s = (
        text.replace(",", ".")
        .replace("\xa0", "")
        .replace("\u00a0", "")
        .replace(" ", "")
    )
    return float(s)


def _find(elem, tag: str):
    res = elem.find(f"{{urn:1C.ru:commerceml_2}}{tag}")
    return res if res is not None else elem.find(tag)


def _findall(elem, tag: str):
    res = elem.findall(f"{{urn:1C.ru:commerceml_2}}{tag}")
    return res if res else elem.findall(tag)


def _elem_text(elem: Optional[ET.Element]) -> str:
    """Текст элемента: itertext() или .text (для XML, где .text может быть None)."""
    if elem is None:
        return ""
    return ("".join(elem.itertext()) or (elem.text or "")).strip()


def _text(elem: Optional[ET.Element]) -> str:
    """Текст элемента (только .find, без namespace)."""
    if elem is None:
        return ""
    return (elem.text or "").strip() or "".join(elem.itertext()).strip()


def _parse_offers_sync_regex(xml_content: str) -> Tuple[List[dict], dict]:
    """Извлечь офферы и типы цен через regex (fallback при проблемах с ET)."""
    price_types_xml: dict = {}
    types_match = re.search(r"<ТипыЦен[^>]*>(.*?)</ТипыЦен>", xml_content, re.DOTALL)
    if types_match:
        block = types_match.group(1)
        for m in re.finditer(
            r"<ТипЦены[^>]*>.*?<Ид[^>]*>(.*?)</Ид>.*?<Наименование[^>]*>(.*?)</Наименование>",
            block,
            re.DOTALL,
        ):
            ext_id = (m.group(1) or "").strip()
            name = (m.group(2) or "").strip()
            if ext_id:
                price_types_xml[ext_id] = name or ext_id
        if not price_types_xml:
            for m in re.finditer(
                r"<Ид[^>]*>(.*?)</Ид>\s*<Наименование[^>]*>(.*?)</Наименование>",
                block,
                re.DOTALL,
            ):
                ext_id = (m.group(1) or "").strip()
                if ext_id:
                    price_types_xml[ext_id] = (m.group(2) or "").strip() or ext_id

    offers_data: List[dict] = []
    offers_block = re.search(
        r"<Предложения[^>]*>(.*?)</Предложения>", xml_content, re.DOTALL
    )
    if not offers_block:
        return offers_data, price_types_xml
    block = offers_block.group(1)
    for offer_str in re.findall(
        r"<Предложение[^>]*>(.*?)</Предложение>", block, re.DOTALL
    ):
        id_m = re.search(r"<Ид[^>]*>(.*?)</Ид>", offer_str, re.DOTALL)
        external_id = (id_m.group(1) or "").strip() if id_m else ""
        if not external_id:
            continue
        price_val = 0.0
        price_type_ext_id: Optional[str] = None
        pv_m = re.search(r"<ЦенаЗаЕдиницу[^>]*>(.*?)</ЦенаЗаЕдиницу>", offer_str)
        if pv_m and pv_m.group(1):
            try:
                price_val = _parse_decimal((pv_m.group(1) or "").strip())
            except ValueError:
                pass
        tid_m = re.search(r"<ИдТипаЦены[^>]*>(.*?)</ИдТипаЦены>", offer_str)
        if tid_m and tid_m.group(1):
            price_type_ext_id = (tid_m.group(1) or "").strip()
        qty: Optional[int] = None
        qty_m = re.search(r"<Количество[^>]*>(.*?)</Количество>", offer_str)
        if qty_m and qty_m.group(1):
            try:
                qty = int(_parse_decimal((qty_m.group(1) or "").strip()))
            except ValueError:
                pass
        if qty is None:
            qty_attr = re.search(r'КоличествоНаСкладе="([^"]*)"', offer_str)
            if qty_attr and qty_attr.group(1):
                try:
                    qty = int(_parse_decimal(qty_attr.group(1)))
                except ValueError:
                    pass
        offers_data.append(
            {
                "external_id": external_id,
                "price": price_val,
                "price_type_ext_id": price_type_ext_id,
                "qty": qty,
            }
        )
    return offers_data, price_types_xml


def _parse_offers_sync(xml_content: str) -> Tuple[List[dict], dict]:
    """Синхронно извлечь из XML список офферов и типы цен. Без namespace, fallback — с namespace."""
    root = ET.fromstring(xml_content)
    packet = root.find(".//ПакетПредложений") or root.find(
        ".//{urn:1C.ru:commerceml_2}ПакетПредложений"
    )
    if packet is None:
        # Fallback: первый дочерний элемент корня может быть пакетом
        for child in root:
            if "ПакетПредложений" in child.tag or child.tag == "ПакетПредложений":
                packet = child
                break
        if packet is None:
            return [], {}

    ns = "{urn:1C.ru:commerceml_2}"
    price_types_xml: dict = {}
    types_el = packet.find("ТипыЦен") or packet.find(ns + "ТипыЦен")
    if types_el is not None:
        for pt in types_el.findall("ТипЦены") or types_el.findall(ns + "ТипЦены"):
            id_el = pt.find("Ид") or pt.find(ns + "Ид")
            name_el = pt.find("Наименование") or pt.find(ns + "Наименование")
            if id_el is not None:
                ext_id = _text(id_el)
                if ext_id:
                    price_types_xml[ext_id] = _text(name_el) or ext_id

    offers_data: List[dict] = []
    offers_el = packet.find("Предложения") or packet.find(ns + "Предложения")
    if offers_el is None:
        for ch in packet:
            if "Предложения" in ch.tag:
                offers_el = ch
                break
    if offers_el is None:
        offers_el = packet
    offers_list = offers_el.findall("Предложение") or offers_el.findall(
        ns + "Предложение"
    )
    if not offers_list:
        offers_list = [
            ch
            for ch in offers_el
            if "Предложение" in ch.tag and ch.tag != "Предложения"
        ]

    def _offer_find(offer_el: ET.Element, tag: str) -> Optional[ET.Element]:
        return offer_el.find(tag) or offer_el.find(ns + tag)

    def _offer_findall(offer_el: ET.Element, tag: str) -> list:
        return offer_el.findall(tag) or offer_el.findall(ns + tag)

    for offer in offers_list:
        id_el = _offer_find(offer, "Ид")
        external_id = _text(id_el) if id_el else ""
        if not external_id:
            continue

        price_val = 0.0
        price_type_ext_id: Optional[str] = None
        for tag in ("ЦенаЗаЕдиницу", "Цена"):
            pe = _offer_find(offer, tag)
            if pe is not None and _text(pe):
                try:
                    price_val = _parse_decimal(_text(pe))
                    break
                except ValueError:
                    pass
        prices_el = _offer_find(offer, "Цены")
        if prices_el is not None:
            for price_el in _offer_findall(prices_el, "Цена"):
                pv_el = _offer_find(price_el, "ЦенаЗаЕдиницу")
                tid_el = _offer_find(price_el, "ИдТипаЦены")
                if pv_el is not None and _text(pv_el):
                    try:
                        price_val = _parse_decimal(_text(pv_el))
                    except ValueError:
                        continue
                    price_type_ext_id = _text(tid_el) if tid_el else None
                    break

        qty: Optional[int] = None
        q_el = _offer_find(offer, "Количество")
        if q_el is not None and _text(q_el):
            try:
                qty = int(_parse_decimal(_text(q_el)))
            except ValueError:
                pass
        if qty is None:
            for sklad in _offer_findall(offer, "Склад"):
                attr = sklad.get("КоличествоНаСкладе")
                if attr is not None:
                    try:
                        qty = int(_parse_decimal(attr))
                        break
                    except ValueError:
                        pass
                child = _offer_find(sklad, "КоличествоНаСкладе")
                if child is not None and _text(child):
                    try:
                        qty = int(_parse_decimal(_text(child)))
                        break
                    except ValueError:
                        pass
            if qty is None:
                sklady = _offer_find(offer, "Склады")
                if sklady is not None:
                    for sklad in _offer_findall(sklady, "Склад"):
                        attr = sklad.get("КоличествоНаСкладе")
                        if attr is not None:
                            try:
                                qty = int(_parse_decimal(attr))
                                break
                            except ValueError:
                                pass

        offers_data.append(
            {
                "external_id": external_id,
                "price": price_val,
                "price_type_ext_id": price_type_ext_id,
                "qty": qty,
            }
        )
    if not offers_data:
        return _parse_offers_sync_regex(xml_content)
    return offers_data, price_types_xml


async def parse_offers_xml(xml_content: str, cashbox_id: int) -> int:
    """Разбор ПакетПредложений: цены в prices, остатки — документом прихода."""
    logger.info(
        f"commerceml Starting offers parse for cashbox {cashbox_id}, XML size={len(xml_content)}"
    )
    try:
        offers_data, price_types_xml = _parse_offers_sync(xml_content)
        logger.info(
            f"commerceml {len(offers_data)} offers and {len(price_types_xml)} price types in XML"
        )
        if not offers_data:
            logger.warning("CommerceML: предложения не найдены в XML")
            return 0

        price_type_by_external_id: dict = {}
        for ext_id, name in price_types_xml.items():
            pt_id = await _get_or_create_price_type_by_name(name, ext_id, cashbox_id)
            if pt_id:
                price_type_by_external_id[ext_id] = pt_id
        if not price_type_by_external_id:
            logger.info(
                "CommerceML: типы цен из XML не сопоставлены; используется тип по умолчанию"
            )

        default_price_type_id = await _get_or_create_default_price_type(cashbox_id)
        owner = await _owner_id_for_cashbox(cashbox_id)
        if not owner:
            logger.warning("CommerceML: не найден owner для cashbox=%s", cashbox_id)

        stock_rows: List[dict] = []
        for item in offers_data:
            row = await _process_offer_data(
                item,
                cashbox_id,
                price_type_by_external_id,
                default_price_type_id,
                owner,
            )
            if row:
                stock_rows.append(row)

        if stock_rows:
            await _create_incoming_document(cashbox_id, stock_rows)
        elif offers_data:
            logger.warning(
                "CommerceML: приход/остатки не созданы — ни один товар из предложений не найден в номенклатуре (сначала загрузите каталог) или нет количества (qty>=0)."
            )

        logger.info("Обработано предложений (цены/остатки): %s", len(offers_data))
        return len(offers_data)
    except Exception as e:
        logger.error("Ошибка парсинга предложений: %s", str(e), exc_info=True)
        raise


async def _get_or_create_price_type_by_name(
    name: str, _external_id: str, cashbox_id: int
) -> Optional[int]:
    """Тип цены по наименованию; создаёт при отсутствии."""
    name = (name or "").strip()
    if not name:
        return None
    q = select(price_types).where(
        price_types.c.cashbox == cashbox_id,
        price_types.c.is_deleted.is_not(True),
    )
    rows = await database.fetch_all(q)
    for r in rows:
        if getattr(r, "name", None) == name:
            return r.id

    owner = await _owner_id_for_cashbox(cashbox_id)
    if not owner:
        return None
    ins = (
        price_types.insert()
        .values(
            name=name,
            owner=owner,
            cashbox=cashbox_id,
            is_deleted=False,
            is_system=False,
        )
        .returning(price_types.c.id)
    )
    pid = await database.execute(ins)
    return int(pid) if pid is not None else None


async def _get_or_create_default_price_type(cashbox_id: int) -> Optional[int]:
    """Тип цены по умолчанию: chatting, иначе Основная, иначе любой."""
    q = select(price_types).where(
        price_types.c.cashbox == cashbox_id,
        price_types.c.is_deleted.is_not(True),
    )
    rows = await database.fetch_all(q)
    for r in rows:
        if getattr(r, "name", None) == _PRICE_TYPE_CHATTING:
            return r.id
    for r in rows:
        if getattr(r, "name", None) == _PRICE_TYPE_FALLBACK:
            return r.id
    if rows:
        return rows[0].id
    owner = await _owner_id_for_cashbox(cashbox_id)
    if not owner:
        return None
    ins = (
        price_types.insert()
        .values(
            name=_PRICE_TYPE_CHATTING,
            owner=owner,
            cashbox=cashbox_id,
            is_deleted=False,
            is_system=False,
        )
        .returning(price_types.c.id)
    )
    pid = await database.execute(ins)
    return int(pid) if pid is not None else None


async def _process_offer_data(
    item: dict,
    cashbox_id: int,
    price_type_by_external_id: dict,
    default_price_type_id: Optional[int],
    owner: Optional[int],
) -> Optional[dict]:
    """Записать цену по данным оффера; при qty > 0 вернуть строку для прихода."""
    external_id = (item.get("external_id") or "").strip()
    if not external_id:
        return None
    nom = await database.fetch_one(
        select(nomenclature).where(
            nomenclature.c.external_id == external_id,
            nomenclature.c.cashbox == cashbox_id,
            nomenclature.c.is_deleted.is_not(True),
        )
    )
    if not nom:
        return None

    price_val = float(item.get("price") or 0)
    price_type_id = None
    if item.get("price_type_ext_id") and price_type_by_external_id:
        price_type_id = price_type_by_external_id.get(item["price_type_ext_id"])
    if price_type_id is None:
        price_type_id = default_price_type_id
    if not price_type_id or not owner:
        pass
    else:
        existing = await database.fetch_one(
            select(prices)
            .where(
                prices.c.nomenclature == nom.id,
                prices.c.cashbox == cashbox_id,
                prices.c.is_deleted.is_not(True),
            )
            .order_by(prices.c.id.desc())
            .limit(1)
        )
        if existing and (
            existing.price_type == price_type_id or existing.price_type is None
        ):
            await database.execute(
                prices.update()
                .where(prices.c.id == existing.id)
                .values(
                    price=price_val,
                    price_type=price_type_id or existing.price_type,
                    updated_at=func.now(),
                )
            )
        else:
            await database.execute(
                prices.insert().values(
                    price_type=price_type_id,
                    price=price_val,
                    nomenclature=nom.id,
                    owner=owner,
                    cashbox=cashbox_id,
                    is_deleted=False,
                )
            )

    qty = item.get("qty")
    if qty is not None and qty >= 0:
        return {
            "nomenclature_id": nom.id,
            "quantity": float(qty),
            "price": price_val,
            "price_type_id": price_type_id,
            "unit": getattr(nom, "unit", None),
        }
    return None


async def _get_default_unit_id() -> Optional[int]:
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
    row = await database.fetch_one(q)
    if row:
        return row.id
    row = await database.fetch_one(select(units.c.id).limit(1))
    return row.id if row else None


async def _get_or_create_org_and_warehouse(cashbox_id: int):
    """Организация и склад для кассы; при отсутствии создаёт по умолчанию. Возвращает (org, wh) или (None, None)."""
    org = await database.fetch_one(
        select(organizations)
        .where(
            organizations.c.cashbox == cashbox_id,
            organizations.c.is_deleted == False,
        )
        .limit(1)
    )
    wh = await database.fetch_one(
        select(warehouses)
        .where(
            warehouses.c.cashbox == cashbox_id,
            warehouses.c.is_deleted == False,
        )
        .limit(1)
    )
    if org and wh:
        return org, wh
    owner = await _owner_id_for_cashbox(cashbox_id)
    if not owner:
        return None, None
    if not org:
        await database.execute(
            organizations.insert().values(
                type="legal",
                short_name="Основная",
                owner=owner,
                cashbox=cashbox_id,
                is_deleted=False,
            )
        )
        org = await database.fetch_one(
            select(organizations)
            .where(
                organizations.c.cashbox == cashbox_id,
                organizations.c.is_deleted == False,
            )
            .limit(1)
        )
    if not wh:
        await database.execute(
            warehouses.insert().values(
                name="Основной склад",
                owner=owner,
                cashbox=cashbox_id,
                status=True,
                is_public=True,
                is_deleted=False,
            )
        )
        wh = await database.fetch_one(
            select(warehouses)
            .where(
                warehouses.c.cashbox == cashbox_id,
                warehouses.c.is_deleted == False,
            )
            .limit(1)
        )
    return org, wh


async def _create_incoming_document(cashbox_id: int, stock_rows: List[dict]) -> None:
    """Документ прихода/расхода по остаткам из CommerceML. Остаток заменяется на значение из файла (не прибавляется)."""
    if not stock_rows:
        return
    nom_ids = [r["nomenclature_id"] for r in stock_rows]
    nom_types = await database.fetch_all(
        nomenclature.select().where(nomenclature.c.id.in_(nom_ids))
    )
    type_by_id = {r["id"]: getattr(r, "type", None) for r in nom_types}
    rows = [r for r in stock_rows if type_by_id.get(r["nomenclature_id"]) != "service"]
    if not rows:
        logger.warning(
            "CommerceML: приходный документ не создан — все позиции с остатками имеют тип «Услуга» (услуги не приходуются)."
        )
        return
    org, wh = await _get_or_create_org_and_warehouse(cashbox_id)
    if not org or not wh:
        logger.warning(
            "CommerceML: приходный документ не создан — нет организации или склада для кассы (должен быть владелец кассы)."
        )
        return
    owner = await database.fetch_one(
        select(users_cboxes_relation)
        .where(users_cboxes_relation.c.cashbox_id == cashbox_id)
        .limit(1)
    )
    created_by = owner.get("user") if owner else None
    default_unit = await _get_default_unit_id()

    # Собираем предыдущие остатки и дельты: остаток заменяется на qty из файла
    enriched: List[dict] = []
    for r in rows:
        unit = r.get("unit") or default_unit
        if not unit:
            nom_row = await database.fetch_one(
                nomenclature.select().where(nomenclature.c.id == r["nomenclature_id"])
            )
            unit = getattr(nom_row, "unit", None) if nom_row else default_unit
        if not unit:
            logger.warning("CommerceML: нет единицы, nom_id=%s", r["nomenclature_id"])
            continue
        qty = float(r["quantity"])
        last_wb = await database.fetch_one(
            warehouse_balances_latest.select().where(
                warehouse_balances_latest.c.warehouse_id == wh.id,
                warehouse_balances_latest.c.nomenclature_id == r["nomenclature_id"],
                warehouse_balances_latest.c.organization_id == org.id,
            )
        )
        prev_current = last_wb.current_amount if last_wb else 0
        prev_incoming = (
            last_wb.incoming_amount if last_wb and last_wb.incoming_amount else 0
        )
        prev_outgoing = (
            last_wb.outgoing_amount if last_wb and last_wb.outgoing_amount else 0
        )
        delta = qty - prev_current
        enriched.append(
            {
                **r,
                "unit": unit,
                "qty": qty,
                "prev_current": prev_current,
                "prev_incoming": prev_incoming,
                "prev_outgoing": prev_outgoing,
                "delta": delta,
            }
        )
    if not enriched:
        return

    incoming_rows = [e for e in enriched if e["delta"] > 0]
    outgoing_rows = [e for e in enriched if e["delta"] < 0]
    if not incoming_rows and not outgoing_rows:
        return

    incoming_doc_id = None
    outgoing_doc_id = None
    incoming_sum = 0.0
    outgoing_sum = 0.0

    if incoming_rows:
        incoming_events = []
        incoming_doc_id = await database.execute(
            docs_warehouse.insert().values(
                operation="incoming",
                organization=org.id,
                warehouse=wh.id,
                status=True,
                cashbox=cashbox_id,
                created_by=created_by,
                is_deleted=False,
            )
        )
        for e in incoming_rows:
            delta = e["delta"]
            price = float(e.get("price") or 0)
            incoming_sum += price * delta
            await database.execute(
                docs_warehouse_goods.insert().values(
                    docs_warehouse_id=incoming_doc_id,
                    nomenclature=e["nomenclature_id"],
                    price_type=e.get("price_type_id"),
                    price=price,
                    quantity=delta,
                    unit=e["unit"],
                )
            )
            await database.execute(
                warehouse_register_movement.insert().values(
                    organization_id=org.id,
                    warehouse_id=wh.id,
                    nomenclature_id=e["nomenclature_id"],
                    document_warehouse_id=incoming_doc_id,
                    amount=delta,
                    type_amount=OperationType.plus,
                    cashbox_id=cashbox_id,
                )
            )
            incoming_events.append(
                {
                    "organization_id": org.id,
                    "warehouse_id": wh.id,
                    "nomenclature_id": e["nomenclature_id"],
                    "cashbox_id": cashbox_id,
                }
            )
        await database.execute(
            docs_warehouse.update()
            .where(docs_warehouse.c.id == incoming_doc_id)
            .values(sum=incoming_sum)
        )
        await publish_balance_recalc_batch(incoming_events)
        logger.info(
            "CommerceML: документ прихода id=%s, позиций %s",
            incoming_doc_id,
            len(incoming_rows),
        )

    if outgoing_rows:
        outgoing_events = []
        outgoing_doc_id = await database.execute(
            docs_warehouse.insert().values(
                operation="outgoing",
                organization=org.id,
                warehouse=wh.id,
                status=True,
                cashbox=cashbox_id,
                created_by=created_by,
                is_deleted=False,
            )
        )
        for e in outgoing_rows:
            delta_abs = abs(e["delta"])
            price = float(e.get("price") or 0)
            outgoing_sum += price * delta_abs
            await database.execute(
                docs_warehouse_goods.insert().values(
                    docs_warehouse_id=outgoing_doc_id,
                    nomenclature=e["nomenclature_id"],
                    price_type=e.get("price_type_id"),
                    price=price,
                    quantity=delta_abs,
                    unit=e["unit"],
                )
            )
            await database.execute(
                warehouse_register_movement.insert().values(
                    organization_id=org.id,
                    warehouse_id=wh.id,
                    nomenclature_id=e["nomenclature_id"],
                    document_warehouse_id=outgoing_doc_id,
                    amount=delta_abs,
                    type_amount=OperationType.minus,
                    cashbox_id=cashbox_id,
                )
            )
            outgoing_events.append(
                {
                    "organization_id": org.id,
                    "warehouse_id": wh.id,
                    "nomenclature_id": e["nomenclature_id"],
                    "cashbox_id": cashbox_id,
                }
            )
        await database.execute(
            docs_warehouse.update()
            .where(docs_warehouse.c.id == outgoing_doc_id)
            .values(sum=outgoing_sum)
        )
        await publish_balance_recalc_batch(outgoing_events)
        logger.info(
            "CommerceML: документ расхода id=%s, позиций %s",
            outgoing_doc_id,
            len(outgoing_rows),
        )


async def _process_offer(
    offer: ET.Element,
    cashbox_id: int,
    price_type_by_external_id: Optional[dict] = None,
    external_id_override: Optional[str] = None,
) -> Optional[dict]:
    """Одно предложение: пишем цену; при qty > 0 возвращаем строку для прихода."""
    external_id = (
        (external_id_override or "").strip() if external_id_override is not None else ""
    )
    if not external_id:
        id_el = offer.find("Ид") or _find(offer, "Ид")
        external_id = _elem_text(id_el) if id_el else ""
    if not external_id:
        return None

    nom_q = select(nomenclature).where(
        nomenclature.c.external_id == external_id,
        nomenclature.c.cashbox == cashbox_id,
        nomenclature.c.is_deleted.is_not(True),
    )
    nom = await database.fetch_one(nom_q)
    if not nom:
        logger.debug("CommerceML offers: товар external_id=%s не найден", external_id)
        return None

    price_val = 0.0
    price_type_id = None
    price_written = False

    # Цена на уровне предложения (МойСклад и др.); формат "234 243,00" — пробелы убираем
    for tag in ("ЦенаЗаЕдиницу", "Цена"):
        pe = _find(offer, tag)
        if pe is not None and pe.text:
            try:
                price_val = _parse_decimal(pe.text)
                break
            except ValueError:
                pass

    prices_el = _find(offer, "Цены")
    if prices_el is not None:
        for price_el in _findall(prices_el, "Цена"):
            price_val_el = _find(price_el, "ЦенаЗаЕдиницу")
            type_id_el = _find(price_el, "ИдТипаЦены")
            if price_val_el is not None and price_val_el.text:
                try:
                    price_val = _parse_decimal(price_val_el.text)
                except ValueError:
                    continue
                # Определяем тип цены: по Ид типа, иначе тип по умолчанию
                price_type_id = None
                if type_id_el and price_type_by_external_id:
                    type_ext_id = _elem_text(type_id_el)
                    if type_ext_id:
                        price_type_id = price_type_by_external_id.get(type_ext_id)
                if price_type_id is None:
                    price_type_id = await _get_or_create_default_price_type(cashbox_id)
                if price_type_id is None:
                    logger.warning(
                        "CommerceML: нет типа цены для cashbox=%s (external_id=%s)",
                        cashbox_id,
                        external_id,
                    )
                    continue

                # Владелец: сначала связь касса-пользователь, потом владелец номенклатуры
                owner = await _owner_id_for_cashbox(cashbox_id)
                if not owner:
                    owner = getattr(nom, "owner", None)
                if not owner:
                    logger.warning(
                        "CommerceML: не найден owner для cashbox=%s, nom_id=%s, цена не будет записана",
                        cashbox_id,
                        nom.id,
                    )
                    continue

                existing_q = (
                    select(prices)
                    .where(
                        prices.c.nomenclature == nom.id,
                        prices.c.cashbox == cashbox_id,
                        prices.c.is_deleted.is_not(True),
                    )
                    .order_by(prices.c.id.desc())
                    .limit(1)
                )
                existing = await database.fetch_one(existing_q)
                if existing and (
                    existing.price_type == price_type_id or existing.price_type is None
                ):
                    await database.execute(
                        prices.update()
                        .where(prices.c.id == existing.id)
                        .values(
                            price=price_val,
                            price_type=price_type_id or existing.price_type,
                            updated_at=func.now(),
                        )
                    )
                    logger.info(
                        "CommerceML offers: updated price "
                        "external_id=%s nom_id=%s price=%s price_type=%s cashbox=%s",
                        external_id,
                        nom.id,
                        price_val,
                        price_type_id or existing.price_type,
                        cashbox_id,
                    )
                else:
                    new_id = await database.execute(
                        prices.insert().values(
                            price_type=price_type_id,
                            price=price_val,
                            nomenclature=nom.id,
                            owner=owner,
                            cashbox=cashbox_id,
                            is_deleted=False,
                        )
                    )
                    logger.info(
                        "CommerceML offers: inserted price "
                        "id=%s external_id=%s nom_id=%s price=%s price_type=%s cashbox=%s",
                        new_id,
                        external_id,
                        nom.id,
                        price_val,
                        price_type_id,
                        cashbox_id,
                    )
                price_written = True
                break

    # Чтобы товар точно отображался на странице /prices — одна запись в prices на предложение
    if not price_written:
        price_type_id = await _get_or_create_default_price_type(cashbox_id)
        owner = await _owner_id_for_cashbox(cashbox_id)
        if not owner:
            owner = getattr(nom, "owner", None)
        if price_type_id and owner:
            existing_q = (
                select(prices)
                .where(
                    prices.c.nomenclature == nom.id,
                    prices.c.cashbox == cashbox_id,
                    prices.c.is_deleted.is_not(True),
                )
                .order_by(prices.c.id.desc())
                .limit(1)
            )
            existing = await database.fetch_one(existing_q)
            if existing:
                await database.execute(
                    prices.update()
                    .where(prices.c.id == existing.id)
                    .values(
                        price=price_val,
                        price_type=price_type_id or existing.price_type,
                        updated_at=func.now(),
                    )
                )
                logger.info(
                    "CommerceML offers: fallback updated price "
                    "external_id=%s nom_id=%s price=%s price_type=%s cashbox=%s",
                    external_id,
                    nom.id,
                    price_val,
                    price_type_id or existing.price_type,
                    cashbox_id,
                )
            else:
                new_id = await database.execute(
                    prices.insert().values(
                        price_type=price_type_id,
                        price=price_val,
                        nomenclature=nom.id,
                        owner=owner,
                        cashbox=cashbox_id,
                        is_deleted=False,
                    )
                )
                logger.info(
                    "CommerceML offers: fallback inserted price "
                    "id=%s external_id=%s nom_id=%s price=%s price_type=%s cashbox=%s",
                    new_id,
                    external_id,
                    nom.id,
                    price_val,
                    price_type_id,
                    cashbox_id,
                )

    qty = None
    qty_el = _find(offer, "Количество")
    if qty_el is not None and qty_el.text:
        try:
            qty = int(_parse_decimal(qty_el.text))
        except ValueError:
            pass
    if qty is None:
        rest_el = _find(offer, "Остатки")
        if rest_el is not None:
            for o in _findall(rest_el, "Остаток"):
                k = _find(o, "Количество")
                if k is not None and k.text:
                    try:
                        qty = int(_parse_decimal(k.text))
                        break
                    except ValueError:
                        pass
    if qty is None:
        for sklad in _findall(offer, "Склад"):
            if sklad is None:
                continue
            attr = sklad.get("КоличествоНаСкладе")
            if attr is not None:
                try:
                    qty = int(_parse_decimal(attr))
                    break
                except ValueError:
                    pass
            # КоличествоНаСкладе как дочерний элемент (МойСклад и др.)
            qty_child = _find(sklad, "КоличествоНаСкладе")
            if qty_child is not None and qty_child.text:
                try:
                    qty = int(_parse_decimal(qty_child.text))
                    break
                except ValueError:
                    pass
        if qty is None:
            # Склады/Склад (вложенная структура)
            sklady_el = _find(offer, "Склады")
            if sklady_el is not None:
                for sklad in _findall(sklady_el, "Склад"):
                    if sklad is None:
                        continue
                    attr = sklad.get("КоличествоНаСкладе")
                    if attr is not None:
                        try:
                            qty = int(_parse_decimal(attr))
                            break
                        except ValueError:
                            pass
                    qty_child = _find(sklad, "КоличествоНаСкладе")
                    if qty_child is not None and qty_child.text:
                        try:
                            qty = int(_parse_decimal(qty_child.text))
                            break
                        except ValueError:
                            pass
                    if qty is not None:
                        break

    if qty is not None and qty > 0:
        unit = getattr(nom, "unit", None)
        return {
            "nomenclature_id": nom.id,
            "quantity": float(qty),
            "price": price_val,
            "price_type_id": price_type_id,
            "unit": unit,
        }
    return None
