from typing import Any, Mapping

from apps.amocrm.leads.models.NewLeadBaseModelMessage import NewLeadBaseModelMessage
from apps.amocrm.leads.repositories.core.ILeadsRepository import ILeadsRepository
from apps.amocrm.leads.repositories.models.CreateLeadModel import (
    CreateLeadModel,
    CustomFieldValue,
    CustomFieldValueElement,
    EmveddedContactModel,
    EmveddedModel,
)
from apps.amocrm.tools.get_install import get_install_by_cashbox
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from database.db import (
    amo_leads,
    amo_leads_docs_sales_mapping,
    booking_tags,
    database,
    docs_sales,
    docs_sales_tags,
    payments,
)
from sqlalchemy import insert, select, update


class PostLeadEvent(IEventHandler[NewLeadBaseModelMessage]):

    def __init__(self, leads_repository: ILeadsRepository):
        self.__leads_repository = leads_repository

    async def __call__(self, event: Mapping[str, Any]):
        post_amo_lead_message = NewLeadBaseModelMessage(**event)

        install_info = await get_install_by_cashbox(
            cashbox_id=post_amo_lead_message.cashbox_id, type_install="leads"
        )

        custom_fields = [
            CustomFieldValue(
                field_code="ACC_LINK",
                values=[
                    CustomFieldValueElement(value=post_amo_lead_message.account_link)
                ],
            ),
            CustomFieldValue(
                field_code="ACT_LINK",
                values=[CustomFieldValueElement(value=post_amo_lead_message.act_link)],
            ),
            CustomFieldValue(
                field_code="NOMEN_INFO",
                values=[
                    CustomFieldValueElement(value=post_amo_lead_message.nomenclature)
                ],
            ),
            CustomFieldValue(
                field_code="AREND_START",
                values=[
                    CustomFieldValueElement(value=post_amo_lead_message.start_period)
                ],
            ),
            CustomFieldValue(
                field_code="AREND_END",
                values=[
                    CustomFieldValueElement(value=post_amo_lead_message.end_period)
                ],
            ),
        ]
        if post_amo_lead_message.contact_id:
            create_lead_model = CreateLeadModel(
                name=post_amo_lead_message.lead_name,
                price=(
                    0
                    if not post_amo_lead_message.price
                    else post_amo_lead_message.price
                ),
                status_id=post_amo_lead_message.status_id,
                custom_fields_values=custom_fields,
                _embedded=EmveddedModel(
                    contacts=[EmveddedContactModel(id=post_amo_lead_message.contact_id)]
                ),
            )
        else:
            create_lead_model = CreateLeadModel(
                name=post_amo_lead_message.lead_name,
                price=(
                    0
                    if not post_amo_lead_message.price
                    else post_amo_lead_message.price
                ),
                status_id=post_amo_lead_message.status_id,
                custom_fields_values=custom_fields,
            )

        created_leads = await self.__leads_repository.create_lead(
            access_token=install_info.access_token,
            amo_lead_model=create_lead_model,
            referrer=install_info.referrer,
        )
        for index, lead_info in enumerate(created_leads):
            query = (
                insert(amo_leads)
                .values(
                    amo_install_group_id=install_info.group_id,
                    name=post_amo_lead_message.lead_name,
                    is_deleted=False,
                    amo_id=lead_info["id"],
                    contact_id=post_amo_lead_message.contact_id,
                )
                .returning(amo_leads.c.id)
            )
            created_lead = await database.fetch_one(query)

            query = insert(amo_leads_docs_sales_mapping).values(
                docs_sales_id=post_amo_lead_message.docs_sales_id,
                lead_id=created_lead.id,
                table_status=1,
                is_sync=True,
                amo_install_group_id=install_info.group_id,
                cashbox_id=post_amo_lead_message.cashbox_id,
            )
            await database.execute(query)

            query = insert(docs_sales_tags).values(
                docs_sales_id=post_amo_lead_message.docs_sales_id,
                name=f"ID_{lead_info['id']}",
            )
            await database.execute(query)

            query = insert(booking_tags).values(
                booking_id=post_amo_lead_message.booking_id,
                name=f"ID_{lead_info['id']}",
            )
            await database.execute(query)

            query = select(docs_sales.c.tags).where(
                docs_sales.c.id == post_amo_lead_message.docs_sales_id
            )
            doc_sale_tags_info = await database.fetch_one(query)
            if doc_sale_tags_info:
                if doc_sale_tags_info.tags:
                    tags = doc_sale_tags_info.tags + f",ID_{lead_info['id']}"
                else:
                    tags = f"ID_{lead_info['id']}"
            else:
                tags = f"ID_{lead_info['id']}"

            query = (
                update(docs_sales)
                .where(docs_sales.c.id == post_amo_lead_message.docs_sales_id)
                .values(tags=tags)
            )
            await database.execute(query)

            query = select(payments.c.tags).where(
                payments.c.docs_sales_id == post_amo_lead_message.docs_sales_id
            )
            payment_tags_info = await database.fetch_one(query)
            if payment_tags_info:
                if payment_tags_info.tags:
                    payment_tags = payment_tags_info.tags + f",ID_{lead_info['id']}"
                else:
                    payment_tags = f"ID_{lead_info['id']}"
            else:
                payment_tags = f"ID_{lead_info['id']}"
            query = (
                update(payments)
                .where(payments.c.docs_sales_id == post_amo_lead_message.docs_sales_id)
                .values(tags=payment_tags)
            )
            await database.execute(query)
