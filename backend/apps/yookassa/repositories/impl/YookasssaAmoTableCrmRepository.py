from apps.yookassa.repositories.core.IYookasssaAmoTableCrmRepository import (
    IYookasssaAmoTableCrmRepository,
)
from database.db import (
    amo_install,
    amo_install_table_cashboxes,
    amo_leads_docs_sales_mapping,
    database,
)
from sqlalchemy import select


class YookasssaAmoTableCrmRepository(IYookasssaAmoTableCrmRepository):
    async def get_active_install_by_cashbox(self, cashbox: int):
        return await database.fetch_one(
            select(
                amo_install_table_cashboxes.c.cashbox_id,
                amo_install_table_cashboxes.c.amo_install_group_id,
                amo_install.c.referrer,
                amo_install.c.id.label("install_id"),
            )
            .where(amo_install_table_cashboxes.c.cashbox_id == cashbox)
            .select_from(amo_install_table_cashboxes)
            .join(
                amo_install,
                amo_install_table_cashboxes.c.amo_install_group_id
                == amo_install.c.install_group_id,
            )
            .where(amo_install.c.active == True)
        )

    async def get_lead_id_by_docs_sales_id(
        self, docs_sales_id: int, amo_install_group_id: int
    ):
        return await database.fetch_val(
            select(amo_leads_docs_sales_mapping.c.lead_id).where(
                amo_leads_docs_sales_mapping.c.docs_sales_id == docs_sales_id,
                amo_leads_docs_sales_mapping.c.amo_install_group_id
                == amo_install_group_id,
            )
        )
