import json
from datetime import datetime, timedelta

from api.articles.schemas import DistributeAccording
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc
from const import PaymentType, SaleOperations
from database.db import (
    articles,
    database,
    distribution_docs_operations,
    docs_sales,
    fifo_settings,
    gross_profit_docs,
    gross_profit_docs_operations,
    organizations,
    payments,
)
from functions.goods_distribution import (
    is_organization_in_progress,
    set_organization_in_progress,
)
from sqlalchemy import desc


async def generate_gross_profit_report(
    organization_id: int,
    period_start: int = 0,
    period_end: int = None,
):
    if await is_organization_in_progress(organization_id):
        return
    query = organizations.select().where(organizations.c.id == organization_id)
    organization = await database.fetch_one(query)
    if not organization:
        raise Exception(f"No such organization: {organization_id}")

    await set_organization_in_progress(organization_id)
    try:
        query = gross_profit_docs.insert(
            {
                "organization": organization_id,
                "period_start": period_start,
                "period_end": period_end,
            }
        )
        gross_profit_doc_id = await database.execute(query)

        query = docs_sales.select().where(
            docs_sales.c.organization == organization_id,
            docs_sales.c.operation != SaleOperations.order,
            docs_sales.c.dated > period_start,
            docs_sales.c.dated < period_end if period_end else True,
        )
        sale_docs = await database.fetch_all(query)

        for sale in sale_docs:
            summary_data = {
                "gross_profit_doc_id": gross_profit_doc_id,
                "document_sale": sale.id,
                "sum": sale.sum,
                "sales_manager": sale.sales_manager,
            }

            query = (
                distribution_docs_operations.select()
                .where(distribution_docs_operations.c.document_sale == sale.id)
                .order_by(desc(distribution_docs_operations.c.created_at))
            )
            distribution_summary = await database.fetch_one(query)
            summary_data["net_cost"] = distribution_summary.end_price

            query = payments.select().where(
                payments.c.docs_sales_id == sale.id,
                payments.c.type == PaymentType.incoming,
            )
            all_incoming_payments = await database.fetch_all(query)
            incoming_payments_sum = sum(
                [payment.amount for payment in all_incoming_payments]
            )
            summary_data["actual_revenue"] = incoming_payments_sum

            query = articles.select().where(
                articles.c.distribute_according == DistributeAccording.costs.value
            )
            costs_articles = await database.fetch_all(query)
            costs_articles_ids = [article.id for article in costs_articles]

            query = payments.select().where(
                payments.c.docs_sales_id == sale.id,
                payments.c.type == PaymentType.outgoing,
                payments.c.article_id.notin_(costs_articles_ids),
            )
            direct_outgoing_payments = await database.fetch_all(query)
            direct_outgoing_payments_sum = sum(
                [payment.amount for payment in direct_outgoing_payments]
            )
            summary_data["direct_costs"] = direct_outgoing_payments_sum

            query = payments.select().where(
                payments.c.docs_sales_id == sale.id,
                payments.c.type == PaymentType.outgoing,
                payments.c.article_id.in_(costs_articles_ids),
            )
            indirect_outgoing_payments = await database.fetch_all(query)
            indirect_outgoing_payments_sum = sum(
                [payment.amount for payment in indirect_outgoing_payments]
            )
            summary_data["indirect_costs"] = indirect_outgoing_payments_sum

            summary_data["gross_profit"] = (
                summary_data["actual_revenue"] - summary_data["net_cost"]
            )

            summary_data["rentability"] = (
                summary_data["sum"]
                - summary_data["direct_costs"]
                - summary_data["indirect_costs"]
                - summary_data["net_cost"]
            ) / summary_data["net_cost"]

            query = gross_profit_docs_operations.insert(summary_data)
            gross_profit_doc_id = await database.execute(query)

    finally:
        await set_organization_in_progress(organization_id, False)


async def process_gross_profit_report():
    query = fifo_settings.select()
    settings = await database.fetch_all(query)
    for setting in settings:
        first_day = datetime(datetime.utcnow().year, datetime.utcnow().month, 1)
        if setting.month_closing_delay_days:
            first_day += timedelta(days=setting.month_closing_delay_days)
        if setting.fully_closed_date < first_day.timestamp():
            first_day_timestamp = int(first_day.timestamp())
            await produce_gross_profit_report(
                organization_id=setting.organization_id,
                period_start=setting.fully_closed_date,
                period_end=int(first_day_timestamp),
            )
            continue

        previous_close_time = (
            int(datetime.utcnow().timestamp()) - setting.preview_close_period_seconds
        )
        if (
            not setting.temporary_closed_date
            or setting.temporary_closed_date <= previous_close_time
        ):
            now_timestamp = int(datetime.utcnow().timestamp())
            await produce_gross_profit_report(
                organization_id=setting.organization_id,
                period_start=setting.fully_closed_date,
                period_end=now_timestamp,
            )


async def produce_gross_profit_report(**body) -> None:
    factory = ioc.get(IRabbitFactory)
    messaging = await factory()
    body["func_name"] = generate_gross_profit_report.__name__
    await messaging.publish_to_queue("report_queue", json.dumps(body).encode())
