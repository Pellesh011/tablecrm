import uuid

from database.db import Base
from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func


class TechOperationDB(Base):
    """Производственная операция, порождённая тех картой."""

    __tablename__ = "tech_operations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    tech_card_id = Column(
        UUID(as_uuid=True), ForeignKey("tech_cards.id"), nullable=False
    )
    output_quantity = Column(Float, nullable=False)

    # Склад откуда берём сырьё
    from_warehouse_id = Column(Integer, nullable=False)
    # Склад куда кладём готовый продукт
    to_warehouse_id = Column(Integer, nullable=False)

    # Привязки
    user_id = Column(Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=False)
    cashbox_id = Column(Integer, ForeignKey("cashboxes.id"), nullable=True)
    nomenclature_id = Column(Integer, ForeignKey("nomenclature.id"), nullable=True)

    # Ссылки на складские документы, созданные этой операцией
    production_doc_id = Column(
        Integer, ForeignKey("docs_warehouse.id"), nullable=True
    )  # Оприходование / Производство
    consumption_doc_id = Column(
        Integer, ForeignKey("docs_warehouse.id"), nullable=True
    )  # Списание

    sale_write_off_doc_id = Column(
        Integer,
        ForeignKey("docs_warehouse.id", ondelete="SET NULL"),
        nullable=True,
        comment="Документ списания проданного товара на складе-получателе (warehouse_to_id)",
    )
    docs_sales_id = Column(Integer, ForeignKey("docs_sales.id"), nullable=True)

    status = Column(
        Enum(
            "active",
            "reversed",
            "canceled",
            "deleted",
            name="tech_op_status",
            create_type=False,
        ),
        default="active",
        nullable=False,
    )

    production_order_id = Column(UUID(as_uuid=True), nullable=True)
    consumption_order_id = Column(UUID(as_uuid=True), nullable=True)

    created_at = Column(DateTime(timezone=True), default=func.now)
    updated_at = Column(DateTime(timezone=True), default=func.now, onupdate=func.now)

    # Relations
    tech_card = relationship("TechCardDB", back_populates="operations")
    components = relationship(
        "TechOperationComponentDB",
        back_populates="operation",
        cascade="all, delete-orphan",
    )
    payments = relationship("TechOperationPaymentDB", back_populates="operation")


class TechOperationComponentDB(Base):
    """Компоненты (сырьё), задействованные в операции."""

    __tablename__ = "tech_operation_components"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operation_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tech_operations.id", ondelete="CASCADE"),
        nullable=False,
    )
    nomeclature_id = Column(Integer, ForeignKey("nomenclature.id"), nullable=True)
    name = Column(String(255), nullable=False)
    quantity = Column(Float, nullable=False)
    gross_weight = Column(Float, nullable=True)
    net_weight = Column(Float, nullable=True)

    operation = relationship("TechOperationDB", back_populates="components")


class TechOperationPaymentDB(Base):
    """Платежи, связанные с операцией."""

    __tablename__ = "tech_operation_payments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    operation_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tech_operations.id", ondelete="CASCADE"),
        nullable=False,
    )
    payment_id = Column(UUID(as_uuid=True), nullable=False)

    operation = relationship("TechOperationDB", back_populates="payments")
