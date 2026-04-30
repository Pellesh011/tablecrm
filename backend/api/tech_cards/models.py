import uuid

from database.db import Base
from sqlalchemy import (
    Boolean,
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


class TechCardDB(Base):
    """Технологическая карта производства."""

    __tablename__ = "tech_cards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=True)

    # Тип карты по роли документа продажи
    card_type = Column(
        Enum("reference", "automatic", name="card_type", create_type=False),
        nullable=False,
    )

    # Режим автоматизации
    card_mode = Column(
        String(20),
        nullable=False,
        default="reference",
        # 'reference' | 'semi_auto' | 'auto'
    )

    auto_produce = Column(Boolean, default=False)

    # Привязки
    user_id = Column(Integer, ForeignKey("relation_tg_cashboxes.id"), nullable=True)
    cashbox_id = Column(Integer, ForeignKey("cashboxes.id"), nullable=True)
    parent_nomenclature_id = Column(
        Integer, ForeignKey("nomenclature.id"), nullable=True
    )

    # Склады для auto / semi_auto
    warehouse_from_id = Column(Integer, ForeignKey("warehouses.id"), nullable=True)
    warehouse_to_id = Column(Integer, ForeignKey("warehouses.id"), nullable=True)

    status = Column(
        Enum("active", "canceled", "deleted", name="status", create_type=False),
        default="active",
    )
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relations
    items = relationship(
        "TechCardItemDB", back_populates="tech_card", cascade="all, delete-orphan"
    )
    output_items = relationship(
        "TechCardOutputItemDB", back_populates="tech_card", cascade="all, delete-orphan"
    )
    operations = relationship("TechOperationDB", back_populates="tech_card")


class TechCardItemDB(Base):
    """Компоненты (сырьё) тех карты."""

    __tablename__ = "tech_card_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tech_card_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tech_cards.id", ondelete="CASCADE"),
        nullable=False,
    )
    nomenclature_id = Column(Integer, ForeignKey("nomenclature.id"), nullable=True)
    type_of_processing = Column(String(255), nullable=True)

    # Потери при обработке (опционально)
    waste_from_cold_processing = Column(Float, nullable=True)
    waste_from_heat_processing = Column(Float, nullable=True)

    net_weight = Column(Float, nullable=True)
    quantity = Column(Float, nullable=False)
    gross_weight = Column(Float, nullable=True)
    output = Column(Float, nullable=True)

    tech_card = relationship("TechCardDB", back_populates="items")


class TechCardOutputItemDB(Base):
    """Выходные изделия тех карты (что производится)."""

    __tablename__ = "tech_card_output_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tech_card_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tech_cards.id", ondelete="CASCADE"),
        nullable=False,
    )
    nomenclature_id = Column(Integer, ForeignKey("nomenclature.id"), nullable=False)
    quantity = Column(Float, nullable=False)
    unit_id = Column(Integer, ForeignKey("units.id"), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    tech_card = relationship("TechCardDB", back_populates="output_items")
