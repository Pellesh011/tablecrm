import asyncio
import logging
import os

logging.basicConfig(level=logging.DEBUG)

from api.apple_wallet.handlers.AppleWalletCardUpdateHandler import (
    AppleWalletCardUpdateHandler,
)
from api.apple_wallet.messages.AppleWalletCardUpdateMessage import (
    AppleWalletCardUpdateMessage,
)
from api.docs_purchases.rabbitmq.handlers.CreatePurchaseAutoExpenseHandler import (
    CreatePurchaseAutoExpenseHandler,
)
from api.docs_purchases.rabbitmq.messages.CreatePurchaseAutoExpenseMessage import (
    CreatePurchaseAutoExpenseMessage,
)
from api.docs_sales.handlers.RecalculateFinancialsHandler import (
    RecalculateFinancialsHandler,
)
from api.docs_sales.handlers.RecalculateLoyaltyPointsHandler import (
    RecalculateLoyaltyPointsHandler,
)
from api.docs_sales.handlers.TechCardWarehouseOperationHandler import (
    TechCardWarehouseOperationHandler,
)
from api.docs_sales.messages.RecalculateFinancialsMessageModel import (
    RecalculateFinancialsMessageModel,
)
from api.docs_sales.messages.RecalculateLoyaltyPointsMessageModel import (
    RecalculateLoyaltyPointsMessageModel,
)
from api.docs_sales.messages.TechCardWarehouseOperationMessage import (
    TechCardWarehouseOperationMessage,
)
from api.marketplace.rabbitmq.handlers.CreateMarketplaceOrderHandler import (
    CreateMarketplaceOrderHandler,
)
from api.marketplace.rabbitmq.messages.CreateMarketplaceOrderMessage import (
    CreateMarketplaceOrderMessage,
)
from apps.amocrm.leads.handlers.impl.PostLeadEvent import PostLeadEvent
from apps.amocrm.leads.models.NewLeadBaseModelMessage import NewLeadBaseModelMessage
from apps.amocrm.leads.repositories.impl.LeadsRepository import LeadsRepository
from apps.booking.repeat.handlers.BookingRepeatHandler import BookingRepeatEvent
from apps.booking.repeat.models.BaseBookingRepeatMessageModel import (
    BaseBookingRepeatMessage,
)
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from common.amqp_messaging.common.impl.models.QueueSettingsModel import (
    QueueSettingsModel,
)
from common.amqp_messaging.common.impl.RabbitFactory import RabbitFactory
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings
from database.db import database
from jobs.warehouse_balance_recalc_job.job import WarehouseBalanceRecalcHandler
from messages.warehouse import WarehouseBalanceRecalcEvent

WORKER_POOL = os.getenv("WORKER_POOL", "light")

HEAVY_QUEUE_SETTINGS = [
    QueueSettingsModel(queue_name="warehouse.balance.recalc", prefetch_count=1),
    QueueSettingsModel(queue_name="recalculate.financials", prefetch_count=1),
    QueueSettingsModel(queue_name="purchase.auto_expense", prefetch_count=1),
    QueueSettingsModel(queue_name="teach_card_operation", prefetch_count=1),
]

LIGHT_QUEUE_SETTINGS = [
    QueueSettingsModel(queue_name="recalculate.loyalty_points", prefetch_count=1),
    QueueSettingsModel(queue_name="booking_repeat_tasks", prefetch_count=1),
    QueueSettingsModel(queue_name="post_amo_lead", prefetch_count=1),
    QueueSettingsModel(queue_name="apple_wallet_card_update", prefetch_count=1),
    QueueSettingsModel(queue_name="create_marketplace_order", prefetch_count=1),
]


async def startup():
    await database.connect()

    rabbit_factory = RabbitFactory(
        settings=RabbitMqSettings(
            rabbitmq_host=os.getenv("RABBITMQ_HOST"),
            rabbitmq_user=os.getenv("RABBITMQ_USER"),
            rabbitmq_pass=os.getenv("RABBITMQ_PASS"),
            rabbitmq_port=os.getenv("RABBITMQ_PORT"),
            rabbitmq_vhost=os.getenv("RABBITMQ_VHOST"),
        )
    )

    rabbitmq_factory = await rabbit_factory()
    rabbitmq_messaging: IRabbitMessaging = await rabbitmq_factory()

    if WORKER_POOL == "heavy":
        await rabbitmq_messaging.subscribe(
            RecalculateFinancialsMessageModel, RecalculateFinancialsHandler()
        )
        await rabbitmq_messaging.subscribe(
            TechCardWarehouseOperationMessage, TechCardWarehouseOperationHandler()
        )
        await rabbitmq_messaging.subscribe(
            CreatePurchaseAutoExpenseMessage, CreatePurchaseAutoExpenseHandler()
        )
        await rabbitmq_messaging.subscribe(
            WarehouseBalanceRecalcEvent, WarehouseBalanceRecalcHandler()
        )
        await rabbitmq_messaging.install(HEAVY_QUEUE_SETTINGS)
    else:
        await rabbitmq_messaging.subscribe(
            RecalculateLoyaltyPointsMessageModel, RecalculateLoyaltyPointsHandler()
        )
        await rabbitmq_messaging.subscribe(
            BaseBookingRepeatMessage,
            BookingRepeatEvent(rabbitmq_messaging_factory=rabbitmq_factory),
        )
        await rabbitmq_messaging.subscribe(
            NewLeadBaseModelMessage, PostLeadEvent(leads_repository=LeadsRepository())
        )
        await rabbitmq_messaging.subscribe(
            AppleWalletCardUpdateMessage, AppleWalletCardUpdateHandler()
        )
        await rabbitmq_messaging.subscribe(
            CreateMarketplaceOrderMessage,
            CreateMarketplaceOrderHandler(rabbitmq_factory),
        )
        await rabbitmq_messaging.install(LIGHT_QUEUE_SETTINGS)

    await asyncio.Future()

    await database.disconnect()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.get_event_loop().run_until_complete(startup())
