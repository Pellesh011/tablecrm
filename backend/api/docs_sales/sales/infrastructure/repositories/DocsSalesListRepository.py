from database.db import contragents, docs_sales, docs_sales_delivery_info
from sqlalchemy import desc, func, select


class DocsSalesListRepository:
    @staticmethod
    def get_doc_sale_by_id(id_doc: int, user_cashbox_id: int):
        return (
            select(*docs_sales.columns, contragents.c.name.label("contragent_name"))
            .select_from(docs_sales)
            .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
            .where(
                docs_sales.c.id == id_doc,
                docs_sales.c.is_deleted.is_not(True),
                docs_sales.c.cashbox == user_cashbox_id,
            )
        )

    @staticmethod
    def base_list_query(user_cashbox_id: int):
        return (
            select(*docs_sales.columns, contragents.c.name.label("contragent_name"))
            .select_from(docs_sales)
            .outerjoin(contragents, docs_sales.c.contragent == contragents.c.id)
            .where(
                docs_sales.c.is_deleted.is_not(True),
                docs_sales.c.cashbox == user_cashbox_id,
            )
            .order_by(desc(docs_sales.c.id))
        )

    @staticmethod
    def base_count_list_query(user_cashbox_id: int):
        return (
            select(func.count())
            .select_from(docs_sales)
            .where(
                docs_sales.c.is_deleted.is_not(True),
                docs_sales.c.cashbox == user_cashbox_id,
            )
        )

    def get_count_list_by_created_date_query(
        self, user_cashbox_id: int, start_of_day: int, end_of_day: int
    ):
        return self.base_count_list_query(user_cashbox_id).where(
            docs_sales.c.dated >= start_of_day,
            docs_sales.c.dated <= end_of_day,
        )

    def get_count_list_by_delivery_date_query(self, user_cashbox_id: int, start, end):
        return (
            self.base_count_list_query(user_cashbox_id)
            .outerjoin(
                docs_sales_delivery_info,
                docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id,
            )
            .where(
                docs_sales_delivery_info.c.delivery_date >= start,
                docs_sales_delivery_info.c.delivery_date <= end,
            )
        )

    def query_by_created_date(self, user_cashbox_id: int, start: int, end: int):
        return self.base_list_query(user_cashbox_id).where(
            docs_sales.c.dated >= start,
            docs_sales.c.dated <= end,
        )

    def query_by_delivery_date(self, user_cashbox_id: int, start, end):
        return (
            self.base_list_query(user_cashbox_id)
            .outerjoin(
                docs_sales_delivery_info,
                docs_sales_delivery_info.c.docs_sales_id == docs_sales.c.id,
            )
            .where(
                docs_sales_delivery_info.c.delivery_date >= start,
                docs_sales_delivery_info.c.delivery_date <= end,
            )
        )
