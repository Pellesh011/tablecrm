import re
import uuid
from datetime import datetime
from typing import Any, Mapping

from api.docs_sales.api.routers import create
from api.docs_sales.schemas import Create, CreateMass, Item
from apps.amocrm.leads.models.NewLeadBaseModelMessage import NewLeadBaseModelMessage
from apps.booking.repeat.models.BaseBookingRepeatMessageModel import (
    BaseBookingRepeatMessage,
)
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging
from database.db import (
    amo_lead_statuses,
    amo_leads,
    booking,
    booking_nomenclature,
    database,
    docs_sales,
    docs_sales_goods,
    entity_to_entity,
    loyality_transactions,
    nomenclature,
    payments,
)
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_, insert, or_, select


class BookingRepeatEvent(IEventHandler[BaseBookingRepeatMessage]):

    def __init__(self, rabbitmq_messaging_factory: IRabbitFactory):
        self.__rabbitmq_messaging_factory = rabbitmq_messaging_factory

    async def __calculate_paid_doc(self, docs_sales_id, cashbox_id):
        query = (
            select(entity_to_entity)
            .where(entity_to_entity.c.cashbox_id == cashbox_id)
            .where(entity_to_entity.c.from_id == docs_sales_id)
        )
        proxyes = await database.fetch_all(query)

        paid_rubles = 0
        paid_loyality = 0

        for proxy in proxyes:
            if proxy.from_entity == 7:

                # Платеж

                if proxy.to_entity == 5:
                    query = select(payments).where(
                        and_(
                            payments.c.id == proxy.to_id,
                            payments.c.cashbox == cashbox_id,
                            payments.c.is_deleted == False,
                        )
                    )
                    payment = await database.fetch_one(query)
                    if payment:
                        paid_rubles += payment.amount

                    # Транзакция
                    if proxy.to_entity == 6:
                        query = select(loyality_transactions).where(
                            and_(
                                loyality_transactions.c.id == proxy.to_id,
                                loyality_transactions.c.cashbox == cashbox_id,
                                loyality_transactions.c.is_deleted == False,
                            )
                        )
                        trans = await database.fetch_one(query)
                        if trans:
                            paid_loyality += trans.amount

        return paid_rubles, paid_loyality

    async def booking_repeat(
        self,
        lead_info: amo_leads,
        booking_repeat_message: BaseBookingRepeatMessage,
        rabbitmq_messaging_instance: IRabbitMessaging,
    ):
        async with database.transaction(force_rollback=True):
            print(booking_repeat_message.dict())
            query = select(booking).where(
                booking.c.id == booking_repeat_message.booking_id
            )
            booking_info = await database.fetch_one(query)

            query = (
                select(nomenclature)
                .join(
                    booking_nomenclature,
                    nomenclature.c.id == booking_nomenclature.c.nomenclature_id,
                )
                .where(
                    and_(
                        booking_nomenclature.c.booking_id
                        == booking_repeat_message.booking_id,
                        nomenclature.c.cashbox == booking_repeat_message.cashbox_id,
                        nomenclature.c.is_deleted == False,
                    )
                )
            )
            nomenclature_info = await database.fetch_one(query)
            new_nomenclature_id = (
                None if not nomenclature_info else nomenclature_info.id
            )
            nomenclature_name = (
                None if not nomenclature_info else nomenclature_info.name
            )

            query = (
                select(docs_sales_goods)
                .join(
                    nomenclature, nomenclature.c.id == docs_sales_goods.c.nomenclature
                )
                .where(
                    and_(
                        docs_sales_goods.c.docs_sales_id == booking_info.docs_sales_id,
                        nomenclature.c.type.in_(["resurs", "dopresurs"]),
                    )
                )
            )
            docs_sales_goods_info_list = await database.fetch_all(query)

            # if not docs_sales_goods_info_list:
            #     print("Чёт не то")
            #     return

            nomenclatures_to_duplicate = [good for good in docs_sales_goods_info_list]

            query = select(docs_sales).where(
                and_(
                    docs_sales.c.cashbox == booking_repeat_message.cashbox_id,
                    docs_sales.c.id == booking_info.docs_sales_id,
                )
            )
            docs_sales_info = await database.fetch_one(query)
            docs_sales_info_dict = dict(docs_sales_info)

            paid_rubles, paid_loyality = await self.__calculate_paid_doc(
                docs_sales_id=booking_info.docs_sales_id,
                cashbox_id=booking_repeat_message.cashbox_id,
            )
            print(paid_rubles, paid_loyality)
            del docs_sales_info_dict["number"]
            del docs_sales_info_dict["dated"]
            del docs_sales_info_dict["status"]
            docs_sales_info_dict["tags"] = re.sub(
                r"(?:^|,)ID_[^,]*", "", docs_sales_info_dict["tags"]
            )
            result = await create(
                token=booking_repeat_message.token,
                docs_sales_data=CreateMass(
                    __root__=[
                        Create(
                            dated=int(datetime.now().timestamp()),
                            paid_rubles=paid_rubles,
                            paid_lt=paid_loyality,
                            status=False,
                            **docs_sales_info_dict,
                            goods=[
                                Item(
                                    nomenclature=docs_sales_goods_info.nomenclature,
                                    price_type=docs_sales_goods_info.price_type,
                                    price=docs_sales_goods_info.price,
                                    quantity=docs_sales_goods_info.quantity,
                                    unit=docs_sales_goods_info.unit,
                                    unit_name=docs_sales_goods_info.unit_name,
                                    tax=docs_sales_goods_info.tax,
                                    discount=docs_sales_goods_info.discount,
                                    sum_discounted=docs_sales_goods_info.sum_discounted,
                                    status=docs_sales_goods_info.status,
                                )
                                for docs_sales_goods_info in nomenclatures_to_duplicate
                            ],
                        )
                    ]
                ),
                generate_out=False if not nomenclatures_to_duplicate else True,
            )

            booking_info_dict = dict(booking_info)
            del booking_info_dict["id"]
            del booking_info_dict["created_at"]
            del booking_info_dict["updated_at"]
            booking_info_dict["date_booking"] = (
                int(
                    (
                        datetime.fromtimestamp(booking_info_dict["date_booking"])
                        + relativedelta(months=1)
                    ).timestamp()
                )
                + 1
            )
            booking_info_dict["start_booking"] = (
                int(
                    (
                        datetime.fromtimestamp(booking_info_dict["start_booking"])
                        + relativedelta(months=1)
                    ).timestamp()
                )
                + 1
            )
            booking_info_dict["end_booking"] = (
                int(
                    (
                        datetime.fromtimestamp(booking_info_dict["end_booking"])
                        + relativedelta(months=1)
                    ).timestamp()
                )
                + 1
            )
            booking_info_dict["docs_sales_id"] = result[0]["id"]
            print(booking_info_dict)
            query = insert(booking).values(**booking_info_dict).returning(booking.c.id)
            new_booking_id = await database.fetch_one(query)

            if new_nomenclature_id:
                query = insert(booking_nomenclature).values(
                    {
                        "booking_id": new_booking_id.id,
                        "nomenclature_id": new_nomenclature_id,
                        "tariff": "month",
                        "is_deleted": False,
                    }
                )
                await database.execute(query)

            query = select(amo_lead_statuses.c.amo_id).where(
                amo_lead_statuses.c.id == lead_info.status_id
            )
            status_id = await database.fetch_one(query)

            await rabbitmq_messaging_instance.publish(
                message=NewLeadBaseModelMessage(
                    message_id=uuid.uuid4(),
                    lead_name=lead_info.name,
                    price=lead_info.price,
                    status_id=status_id.amo_id,
                    contact_id=lead_info.contact_id,
                    account_link="https://www.drom.ru",
                    act_link="https://www.drom.ru",
                    nomenclature=(
                        "Не указано" if not nomenclature_name else nomenclature_name
                    ),
                    start_period=booking_info_dict["start_booking"],
                    end_period=booking_info_dict["end_booking"],
                    docs_sales_id=result[0]["id"],
                    cashbox_id=booking_repeat_message.cashbox_id,
                    booking_id=new_booking_id.id,
                ),
                routing_key="post_amo_lead",
            )

    async def __call__(self, event: Mapping[str, Any]):
        rabbitmq_messaging: IRabbitMessaging = await self.__rabbitmq_messaging_factory()
        booking_repeat_message = BaseBookingRepeatMessage(**event)
        query = select(booking_nomenclature).where(
            and_(
                booking_nomenclature.c.booking_id == booking_repeat_message.booking_id,
                booking_nomenclature.c.is_deleted == False,
            )
        )
        booking_nomenclature_info = await database.fetch_one(query)

        query = select(amo_leads).where(
            amo_leads.c.id == booking_repeat_message.lead_id
        )
        lead_info = await database.fetch_one(query)

        if booking_nomenclature_info:
            original_start_dt = datetime.fromtimestamp(
                booking_repeat_message.start_booking
            )
            original_end_dt = datetime.fromtimestamp(booking_repeat_message.end_booking)

            # Вычисление нового периода бронирования через месяц
            new_start_dt = original_start_dt + relativedelta(months=1)
            new_end_dt = original_end_dt + relativedelta(months=1)

            new_start = int(new_start_dt.timestamp()) + 1
            new_end = int(new_end_dt.timestamp()) + 1

            query = (
                select(booking)
                .join(
                    booking_nomenclature,
                    booking.c.id == booking_nomenclature.c.booking_id,
                )
                .where(
                    and_(
                        booking.c.cashbox == booking_repeat_message.cashbox_id,
                        booking.c.is_deleted == False,
                        booking_nomenclature.c.nomenclature_id
                        == booking_nomenclature_info.nomenclature_id,
                        booking_nomenclature.c.is_deleted == False,
                        booking.c.id != booking_repeat_message.booking_id,
                    )
                )
                .where(
                    or_(
                        and_(
                            booking.c.start_booking <= new_start,
                            booking.c.end_booking >= new_start,
                        ),
                        and_(
                            booking.c.start_booking <= new_end,
                            booking.c.end_booking >= new_end,
                        ),
                        and_(
                            booking.c.start_booking >= new_start,
                            booking.c.end_booking <= new_end,
                        ),
                    )
                )
            )
            find_booking = await database.fetch_one(query)

            if not find_booking:
                await self.booking_repeat(
                    lead_info=lead_info,
                    booking_repeat_message=booking_repeat_message,
                    rabbitmq_messaging_instance=rabbitmq_messaging,
                )
            else:
                print("REPEAT DENIED")
        else:
            await self.booking_repeat(
                lead_info=lead_info,
                booking_repeat_message=booking_repeat_message,
                rabbitmq_messaging_instance=rabbitmq_messaging,
            )
