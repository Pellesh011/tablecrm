class IYookasssaAmoTableCrmRepository:
    async def get_active_install_by_cashbox(self, cashbox: int):
        raise NotImplementedError

    async def get_lead_id_by_docs_sales_id(
        self, docs_sales_id: int, amo_install_group_id: int
    ):
        raise NotImplementedError
