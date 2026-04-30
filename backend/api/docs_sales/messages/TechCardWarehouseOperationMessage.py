from typing import List, Optional

from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class TechCardComponentItem(BaseModelMessage):
    nomenclature_id: int
    quantity: float
    name: Optional[str] = None


class TechCardOutputItem(BaseModelMessage):
    nomenclature_id: int
    quantity: float


class TechCardWarehouseOperationMessage(BaseModelMessage):
    """
    Сообщение для worker-а, создающего складские документы
    на основании типа тех карты.

    card_mode:
      'semi_auto' → создать docs_warehouse "Списание" (сырьё)
      'auto'      → создать TechOperation → два docs_warehouse
    """

    # Идентификаторы
    docs_sale_id: int
    tech_card_id: str  # UUID as str
    cashbox_id: int
    organization_id: int
    user_id: int  # relation_tg_cashboxes.id

    # Режим тех карты
    card_mode: str  # 'semi_auto' | 'auto'

    # Склады
    warehouse_from_id: int
    warehouse_to_id: Optional[int] = None

    # Сырьё для списания
    components: List[TechCardComponentItem] = []

    # Выходные изделия (для auto)
    output_items: List[TechCardOutputItem] = []

    # Готовый продукт (проданный номенклатура для semi_auto)
    sold_nomenclature_id: Optional[int] = None
    sold_quantity: Optional[float] = None
