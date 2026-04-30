from database.db import metadata
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

cdek_credentials = Table(
    "cdek_credentials",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("account", String(255), nullable=False),
    Column("secure_password", String(255), nullable=False),
    Column("access_token", String(1024), nullable=True),
    Column("token_expires_at", DateTime, nullable=True),
    Column(
        "integration_cashboxes",
        Integer,
        ForeignKey("integrations_to_cashbox.id"),
        nullable=False,
        unique=True,
    ),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

cdek_cashbox_settings = Table(
    "cdek_cashbox_settings",
    metadata,
    Column("cashbox_id", Integer, ForeignKey("cashboxes.id"), primary_key=True),
    Column("lk_token", String(1024), nullable=True),
    Column("lk_token_expires_at", DateTime(timezone=True), nullable=True),
    Column("template", JSONB, nullable=True),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

cdek_orders = Table(
    "cdek_orders",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("order_uuid", String(36), nullable=False, unique=True),
    Column("cdek_number", String(50), nullable=True),
    Column("number", String(50), nullable=True),
    Column("status", String(50), nullable=True),
    Column("status_date", DateTime, nullable=True),
    Column("tariff_code", Integer, nullable=True),
    Column("delivery_sum", Float, nullable=True),
    Column("comment", Text, nullable=True),
    Column("recipient_name", String(255), nullable=True),
    Column("recipient_phone", String(50), nullable=True),
    Column("delivery_point", String(50), nullable=True),
    Column("is_reverse", Boolean, server_default="false"),
    Column("raw_data", Text, nullable=True),
    Column("cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False),
    Column("doc_sales_id", Integer, ForeignKey("docs_sales.id"), nullable=True),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

cdek_order_status_history = Table(
    "cdek_order_status_history",
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "order_uuid", String(36), ForeignKey("cdek_orders.order_uuid"), nullable=False
    ),
    Column("status_code", String(50), nullable=False),
    Column("status_name", String(255), nullable=True),
    Column("date_time", DateTime, nullable=False),
    Column("city", String(255), nullable=True),
    Column("reason_code", String(50), nullable=True),
    Column("reason_description", String(255), nullable=True),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
)
