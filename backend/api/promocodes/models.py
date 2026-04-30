import enum

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
from sqlalchemy.sql import func


class PromocodeType(str, enum.Enum):
    ONE_TIME = "one_time"
    PERMANENT = "permanent"


class PromocodeDB(Base):
    __tablename__ = "promocodes"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, nullable=False)
    points_amount = Column(Float, nullable=False)
    type = Column(
        Enum(PromocodeType, name="promocodetype", create_type=False),
        default=PromocodeType.PERMANENT,
        nullable=False,
    )
    max_usages = Column(Integer, nullable=True)
    current_usages = Column(Integer, default=0, server_default="0", nullable=False)
    organization_id = Column(Integer, ForeignKey("organizations.id"), nullable=False)
    distributor_id = Column(Integer, ForeignKey("contragents.id"), nullable=True)
    creator_id = Column(Integer, ForeignKey("tg_accounts.id"), nullable=False)
    valid_after = Column(DateTime(timezone=True), nullable=True)
    valid_until = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
