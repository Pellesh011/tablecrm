from apps.yookassa.repositories.core.IYookassaTableDocsSales import (
    IYookassaTableDocsSales,
)


class YookassaTableDocsSales(IYookassaTableDocsSales):
    async def fetch_goods_by_docs_sales_id(self, doc_sales_id: int):
        pass
