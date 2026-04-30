import os
import uuid
from typing import Iterable, Mapping

from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.impl.RabbitFactory import RabbitFactory
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings
from common.utils.ioc.ioc import ioc
from messages.warehouse import WarehouseBalanceRecalcEvent


def get_warehouse_rabbitmq_factory() -> RabbitFactory:
    return RabbitFactory(
        settings=RabbitMqSettings(
            rabbitmq_host=os.getenv("RABBITMQ_HOST"),
            rabbitmq_user=os.getenv("RABBITMQ_USER"),
            rabbitmq_pass=os.getenv("RABBITMQ_PASS"),
            rabbitmq_port=os.getenv("RABBITMQ_PORT"),
            rabbitmq_vhost=os.getenv("RABBITMQ_VHOST"),
        )
    )


async def publish_balance_recalc(
    rabbitmq_messaging,
    organization_id: int,
    warehouse_id: int,
    nomenclature_id: int,
    cashbox_id: int,
) -> None:
    await rabbitmq_messaging.publish(
        WarehouseBalanceRecalcEvent(
            message_id=uuid.uuid4(),
            organization_id=organization_id,
            warehouse_id=warehouse_id,
            nomenclature_id=nomenclature_id,
            cashbox_id=cashbox_id,
        ),
        routing_key="warehouse.balance.recalc",
    )


async def publish_balance_recalc_with_new_connection(
    organization_id: int,
    warehouse_id: int,
    nomenclature_id: int,
    cashbox_id: int,
) -> None:
    await publish_balance_recalc_batch(
        [
            {
                "organization_id": organization_id,
                "warehouse_id": warehouse_id,
                "nomenclature_id": nomenclature_id,
                "cashbox_id": cashbox_id,
            }
        ]
    )


async def publish_balance_recalc_batch(events: list[dict]) -> None:
    if not events:
        return
    factory = ioc.get(IRabbitFactory)
    messaging = await factory()
    for event in events:
        await publish_balance_recalc(
            rabbitmq_messaging=messaging,
            organization_id=event["organization_id"],
            warehouse_id=event["warehouse_id"],
            nomenclature_id=event["nomenclature_id"],
            cashbox_id=event["cashbox_id"],
        )


def build_balance_recalc_events(
    rows: Iterable[Mapping],
    *,
    organization_key: str = "organization_id",
    warehouse_key: str = "warehouse_id",
    nomenclature_key: str = "nomenclature_id",
    cashbox_key: str = "cashbox_id",
) -> list[dict]:
    unique_keys = set()
    events = []

    for row in rows:
        organization_id = row.get(organization_key)
        warehouse_id = row.get(warehouse_key)
        nomenclature_id = row.get(nomenclature_key)
        cashbox_id = row.get(cashbox_key)

        if (
            organization_id is None
            or warehouse_id is None
            or nomenclature_id is None
            or cashbox_id is None
        ):
            continue

        key = (organization_id, warehouse_id, nomenclature_id, cashbox_id)
        if key in unique_keys:
            continue

        unique_keys.add(key)
        events.append(
            {
                "organization_id": organization_id,
                "warehouse_id": warehouse_id,
                "nomenclature_id": nomenclature_id,
                "cashbox_id": cashbox_id,
            }
        )

    return events
