import logging
from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from common.amqp_messaging.common.core.EventHandler import IEventHandler

logger = logging.getLogger(__name__)


class TechCardWarehouseOperationHandler(IEventHandler):
    """
    Обрабатывает сообщение о необходимости создания складских документов
    на основании тех карты.
    """

    async def __call__(
        self,
        event: Mapping[str, Any],
        message: Optional[IncomingMessage] = None,
    ):
        print("=== TechCardWarehouseOperationHandler: получил сообщение ===")
        print(f"event: {event}")
        try:
            docs_sale_id: int = event["docs_sale_id"]
            tech_card_id: str = event["tech_card_id"]
            cashbox_id: int = event["cashbox_id"]
            organization_id: int = event["organization_id"]
            user_id: int = event["user_id"]
            card_mode: str = event["card_mode"]
            warehouse_from_id: int = event["warehouse_from_id"]
            warehouse_to_id: Optional[int] = event.get("warehouse_to_id")
            components: list = event.get("components", [])
            output_items: list = event.get("output_items", [])
            sold_nomenclature_id: Optional[int] = event.get("sold_nomenclature_id")
            sold_quantity: Optional[float] = event.get("sold_quantity")

            logger.info(
                "TechCardWarehouseOperationHandler: sale=%s card=%s mode=%s",
                docs_sale_id,
                tech_card_id,
                card_mode,
            )

            if card_mode == "semi_auto":
                print("=== card_mode == 'semi_auto' ===")
                await self._handle_semi_auto(
                    docs_sale_id=docs_sale_id,
                    cashbox_id=cashbox_id,
                    organization_id=organization_id,
                    user_id=user_id,
                    warehouse_from_id=warehouse_from_id,
                    components=components,
                )

            elif card_mode == "auto":
                print("=== card_mode == 'auto' ===")
                await self._handle_auto(
                    tech_card_id=tech_card_id,
                    docs_sale_id=docs_sale_id,
                    cashbox_id=cashbox_id,
                    organization_id=organization_id,
                    user_id=user_id,
                    warehouse_from_id=warehouse_from_id,
                    warehouse_to_id=warehouse_to_id,
                    components=components,
                    output_items=output_items,
                    sold_nomenclature_id=sold_nomenclature_id,
                    sold_quantity=sold_quantity,
                )
            else:
                logger.info("card_mode='%s' — нет автоматических действий", card_mode)

        except Exception as exc:
            print(f"Ошибка в обработчике: {exc}")
            logger.exception("Ошибка в TechCardWarehouseOperationHandler: %s", exc)
            raise

    async def _handle_semi_auto(
        self,
        docs_sale_id: int,
        cashbox_id: int,
        organization_id: int,
        user_id: int,
        warehouse_from_id: int,
        components: list,
    ):
        print("=== _handle_semi_auto: создаём списание ===")
        from api.tech_operations.services import create_write_off_doc

        if not components:
            logger.warning(
                "semi_auto: нет компонентов для списания, sale=%s", docs_sale_id
            )
            return

        doc_id = await create_write_off_doc(
            cashbox_id=cashbox_id,
            organization_id=organization_id,
            warehouse_id=warehouse_from_id,
            created_by=user_id,
            components=components,  # здесь уже есть nomenclature_id
            docs_sales_id=docs_sale_id,
        )
        logger.info("semi_auto: создан docs_warehouse Списание id=%s", doc_id)

    async def _handle_auto(
        self,
        tech_card_id: str,
        docs_sale_id: int,
        cashbox_id: int,
        organization_id: int,
        user_id: int,
        warehouse_from_id: int,
        warehouse_to_id: Optional[int],
        components: list,
        output_items: list,
        sold_nomenclature_id: Optional[int],
        sold_quantity: Optional[float],
    ):
        print("=== _handle_auto: создаём техоперацию ===")
        from api.tech_operations.services import (
            create_tech_operation,
        )

        if not warehouse_to_id:
            logger.error("auto: warehouse_to_id не задан, sale=%s", docs_sale_id)
            return

        effective_output = output_items or (
            [{"nomenclature_id": sold_nomenclature_id, "quantity": sold_quantity}]
            if sold_nomenclature_id and sold_quantity
            else []
        )

        result = await create_tech_operation(
            tech_card_id=tech_card_id,
            cashbox_id=cashbox_id,
            organization_id=organization_id,
            user_id=user_id,
            from_warehouse_id=warehouse_from_id,
            to_warehouse_id=warehouse_to_id,
            components=components,
            output_items=effective_output,
            output_quantity=sum(i["quantity"] for i in effective_output),
            nomenclature_id=sold_nomenclature_id,
            docs_sales_id=docs_sale_id,
        )
        logger.info("auto: тех операция создана %s", result)
