PAID = "paid"
DEMO = "demo"
BLOCKED = "blocked"
PAY_LINK = "t.me/pay_bot"
SUCCESS = "success"
cheque_service_url = "https://proverkacheka.com/api/v1/check/get"
report_queue_name = "report_queue"
marketplace_orders_queue_name = "marketplace_orders_queue"


class PaymentType:
    incoming = "incoming"
    outgoing = "outgoing"
    transfer = "transfer"


class RepeatPeriod:
    YEARLY = "yearly"
    MONTHLY = "monthly"
    WEEKLY = "weekly"
    DAILY = "daily"
    HOURLY = "hourly"
    SECONDS = "seconds"


class WarehouseOperations:
    internal_consumption = "Внутреннее потребление"
    surplus_posting = "Оприходование излишков"
    movement_out = "Перемещение из"
    movement_in = "Перемещение в"
    write_off = "Списание"


class SaleOperations:
    order = "Заказ"
    realization = "Реализация"
