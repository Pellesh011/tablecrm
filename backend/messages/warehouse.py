from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class WarehouseBalanceRecalcEvent(BaseModelMessage):
    organization_id: int
    warehouse_id: int
    nomenclature_id: int
    cashbox_id: int
