"""Генератор заказов для CommerceML"""

import logging
from datetime import datetime
from xml.sax.saxutils import escape

from database.db import (
    contragents,
    database,
    docs_sales,
    docs_sales_goods,
    nomenclature,
)
from sqlalchemy import and_, select

logger = logging.getLogger(__name__)


async def generate_orders_xml(cashbox_id: int) -> tuple[str, int]:
    """XML заказов CommerceML 2.10. Возвращает (xml_str, count)."""
    try:
        date_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        query = (
            select(docs_sales)
            .where(
                and_(
                    docs_sales.c.cashbox == cashbox_id,
                    docs_sales.c.is_deleted == False,
                    docs_sales.c.status == False,
                )
            )
            .order_by(docs_sales.c.dated.desc())
            .limit(100)
        )

        orders = await database.fetch_all(query)

        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>\n',
            f'<КоммерческаяИнформация ВерсияСхемы="2.10" ДатаФормирования="{date_str}">\n',
        ]
        for order in orders:
            await add_order_to_xml(order, parts, cashbox_id)
        parts.append("</КоммерческаяИнформация>\n")

        return "".join(parts), len(orders)
    except Exception as e:
        logger.error(f"Ошибка генерации заказов: {str(e)}", exc_info=True)
        raise


async def add_order_to_xml(order, parts: list, cashbox_id: int):
    """Один заказ в XML."""
    try:
        if order.dated:
            dt = datetime.fromtimestamp(order.dated)
            date_str = dt.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            date_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        parts.append("  <Документ>\n")
        parts.append(f"    <Ид>{escape(str(order.id))}</Ид>\n")
        parts.append(f"    <Номер>{escape(order.number or str(order.id))}</Номер>\n")
        parts.append(f"    <Дата>{date_str}</Дата>\n")
        parts.append("    <ХозОперация>Заказ товара</ХозОперация>\n")

        if order.contragent:
            contragent_query = select(contragents).where(
                contragents.c.id == order.contragent
            )
            contragent = await database.fetch_one(contragent_query)
            if contragent:
                parts.append("    <Контрагенты>\n")
                parts.append("      <Контрагент>\n")
                ext_id = getattr(contragent, "external_id", None) or str(contragent.id)
                parts.append(f"        <Ид>{escape(str(ext_id))}</Ид>\n")
                parts.append(
                    f"        <Наименование>{escape(contragent.name or '')}</Наименование>\n"
                )
                parts.append("      </Контрагент>\n")
                parts.append("    </Контрагенты>\n")

        goods_query = select(docs_sales_goods).where(
            docs_sales_goods.c.docs_sales_id == order.id
        )
        goods = await database.fetch_all(goods_query)

        if goods:
            parts.append("    <Товары>\n")
            for good in goods:
                nom_row = await database.fetch_one(
                    select(nomenclature).where(nomenclature.c.id == good.nomenclature)
                )
                nom_id_cml = (nom_row.external_id if nom_row else None) or str(
                    good.nomenclature
                )
                nom_name = (
                    nom_row.name if nom_row else None
                ) or f"Товар {good.nomenclature}"
                parts.append("      <Товар>\n")
                parts.append(f"        <Ид>{escape(str(nom_id_cml))}</Ид>\n")
                parts.append(
                    f"        <Наименование>{escape(nom_name)}</Наименование>\n"
                )
                parts.append(f"        <Количество>{good.quantity}</Количество>\n")
                parts.append(f"        <ЦенаЗаЕдиницу>{good.price}</ЦенаЗаЕдиницу>\n")
                parts.append(f"        <Сумма>{good.quantity * good.price}</Сумма>\n")
                parts.append("      </Товар>\n")
            parts.append("    </Товары>\n")

        parts.append("  </Документ>\n")
    except Exception as e:
        logger.error(f"Ошибка добавления заказа в XML: {str(e)}", exc_info=True)
