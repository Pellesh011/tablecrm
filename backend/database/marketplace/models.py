"""
Marketplace table models.

This module contains all marketplace-related SQLAlchemy table definitions.
All tables use the shared metadata from database.db.
"""

import sqlalchemy
from sqlalchemy import (
    ARRAY,
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)

# Import metadata from main db module
# This works because:
# 1. metadata is defined early in db.py (line 190)
# 2. marketplace models are imported at the END of db.py (line 3251)
# 3. So by the time this import happens, metadata already exists
# However, to avoid circular import when importing directly from marketplace module,
# we use a try-except fallback
try:
    from database.db import metadata
except (ImportError, AttributeError):
    # Fallback: if db is not fully loaded yet, create a new metadata
    # This should not happen in normal usage, but provides safety
    metadata = sqlalchemy.MetaData()

# Marketplace cart tables
marketplace_contragent_cart = sqlalchemy.Table(
    "marketplace_contragent_cart",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "contragent_id",
        Integer,
        ForeignKey("contragents.id"),
        nullable=False,
        unique=True,
    ),
)

marketplace_cart_goods = sqlalchemy.Table(
    "marketplace_cart_goods",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column(
        "nomenclature_id", Integer, ForeignKey("nomenclature.id"), nullable=False
    ),
    sqlalchemy.Column(
        "warehouse_id", Integer, ForeignKey("warehouses.id"), nullable=True
    ),
    sqlalchemy.Column("quantity", Integer, nullable=False),
    sqlalchemy.Column(
        "cart_id",
        BigInteger,
        ForeignKey("marketplace_contragent_cart.id"),
        nullable=False,
    ),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
    sqlalchemy.UniqueConstraint(
        "nomenclature_id",
        "warehouse_id",
        "cart_id",
        name="ux_marketplace_cart_goods_nomenclature_id_warehouse_id_cart_id",
    ),
)

# Marketplace rating and reviews
marketplace_rating_aggregates = sqlalchemy.Table(
    "marketplace_rating_aggregates",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("entity_type", String, nullable=False),
    sqlalchemy.Column("avg_rating", Float, nullable=False),
    sqlalchemy.Column("reviews_count", Integer, nullable=False),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

marketplace_reviews = sqlalchemy.Table(
    "marketplace_reviews",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("entity_type", String, nullable=False),
    sqlalchemy.Column(
        "client_id",
        BigInteger,
        ForeignKey("marketplace_clients_list.id"),
        nullable=False,
    ),
    sqlalchemy.Column("rating", Integer, nullable=False),
    sqlalchemy.Column("text", Text, nullable=False),
    sqlalchemy.Column("status", String, nullable=False, server_default="visible"),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)

# Marketplace UTM tags
marketplace_utm_tags = sqlalchemy.Table(
    "marketplace_utm_tags",
    metadata,
    sqlalchemy.Column(
        "id", BigInteger, primary_key=True, index=True, autoincrement=True
    ),
    sqlalchemy.Column("entity_type", String, nullable=True),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("utm_source", String, nullable=True),
    sqlalchemy.Column("utm_medium", String, nullable=True),
    sqlalchemy.Column("utm_campaign", String, nullable=True),
    sqlalchemy.Column("utm_term", ARRAY(item_type=String), nullable=True),
    sqlalchemy.Column("utm_content", String, nullable=True),
    sqlalchemy.Column("utm_name", String, nullable=True),
    sqlalchemy.Column("utm_phone", String, nullable=True),
    sqlalchemy.Column("utm_email", String, nullable=True),
    sqlalchemy.Column("utm_leadid", String, nullable=True),
    sqlalchemy.Column("utm_yclientid", String, nullable=True),
    sqlalchemy.Column("utm_gaclientid", String, nullable=True),
)

# Marketplace view events
marketplace_view_events = sqlalchemy.Table(
    "marketplace_view_events",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, index=True),
    sqlalchemy.Column(
        "cashbox_id", Integer, ForeignKey("cashboxes.id"), nullable=False
    ),
    sqlalchemy.Column("entity_type", String, nullable=False),
    sqlalchemy.Column("entity_id", Integer, nullable=False),
    sqlalchemy.Column("listing_pos", Integer, nullable=True),
    sqlalchemy.Column("listing_page", Integer, nullable=True),
    sqlalchemy.Column(
        "client_id",
        BigInteger,
        ForeignKey("marketplace_clients_list.id"),
        nullable=False,
    ),
    sqlalchemy.Column("event", String, nullable=False, server_default="view"),
    sqlalchemy.Column("created_at", DateTime(timezone=True), server_default=func.now()),
)
