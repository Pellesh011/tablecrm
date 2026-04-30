"""reworking marketplace models

Revision ID: 16d87642e03c
Revises: 4ffedbdead8a
Create Date: 2025-12-22 08:55:00.676437

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "16d87642e03c"
down_revision = "4ffedbdead8a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "marketplace_clients_list",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("phone", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_marketplace_clients_list_id"),
        "marketplace_clients_list",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_marketplace_clients_list_phone"),
        "marketplace_clients_list",
        ["phone"],
        unique=True,
    )
    op.create_table(
        "marketplace_carts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("phone", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["phone"],
            ["marketplace_clients_list.phone"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_marketplace_carts_id"), "marketplace_carts", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_marketplace_carts_phone"), "marketplace_carts", ["phone"], unique=True
    )
    op.create_table(
        "marketplace_favorites",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("phone", sa.String(), nullable=False),
        sa.Column("entity_type", sa.String(length=50), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["phone"],
            ["marketplace_clients_list.phone"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "phone",
            "entity_type",
            "entity_id",
            name="uq_marketplace_favorites_client_entity",
        ),
    )
    op.create_index(
        op.f("ix_marketplace_favorites_id"),
        "marketplace_favorites",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_marketplace_favorites_phone"),
        "marketplace_favorites",
        ["phone"],
        unique=False,
    )
    op.create_table(
        "marketplace_orders",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("phone", sa.String(), nullable=False),
        sa.Column(
            "status", sa.String(length=32), server_default="created", nullable=False
        ),
        sa.Column(
            "delivery_info", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column(
            "additional_data",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["phone"],
            ["marketplace_clients_list.phone"],
            onupdate="CASCADE",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_marketplace_orders_id"), "marketplace_orders", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_marketplace_orders_phone"),
        "marketplace_orders",
        ["phone"],
        unique=False,
    )
    op.create_table(
        "marketplace_searches",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("phone", sa.String(), nullable=True),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("filters", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("results_count", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["phone"],
            ["marketplace_clients_list.phone"],
            onupdate="CASCADE",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_marketplace_searches_id"), "marketplace_searches", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_marketplace_searches_phone"),
        "marketplace_searches",
        ["phone"],
        unique=False,
    )
    op.drop_index(
        "ix_marketplace_contragent_cart_id", table_name="marketplace_contragent_cart"
    )
    op.drop_constraint(
        "marketplace_contragent_cart_contragent_id_key",
        "marketplace_contragent_cart",
        type_="unique",
    )
    op.drop_constraint(
        "marketplace_cart_goods_cart_id_fkey",
        "marketplace_cart_goods",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "marketplace_cart_goods_cart_id_fkey",
        "marketplace_cart_goods",
        "marketplace_carts",
        ["cart_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_table("marketplace_contragent_cart")
    op.drop_index("ix_favorites_nomenclatures_id", table_name="favorites_nomenclatures")
    op.drop_table("favorites_nomenclatures")
    op.drop_constraint(
        "ux_marketplace_cart_goods_nomenclature_id_warehouse_id_cart_id",
        "marketplace_cart_goods",
        type_="unique",
    )
    op.create_unique_constraint(
        "ux_marketplace_cart_goods_nomenclature_id_warehouse_id_cart_id",
        "marketplace_cart_goods",
        ["nomenclature_id", "warehouse_id", "cart_id"],
    )
    op.add_column(
        "marketplace_utm_tags",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("marketplace_utm_tags", "created_at")
    op.drop_constraint(
        "ux_marketplace_cart_goods_nomenclature_id_warehouse_id_cart_id",
        "marketplace_cart_goods",
        type_="unique",
    )
    op.create_index(
        "ux_marketplace_cart_goods_nomenclature_id_warehouse_id_cart_id",
        "marketplace_cart_goods",
        ["nomenclature_id", "warehouse_id", "cart_id"],
        unique=False,
    )
    op.create_table(
        "marketplace_contragent_cart",
        sa.Column(
            "id",
            sa.BIGINT(),
            server_default=sa.text(
                "nextval('marketplace_contragent_cart_id_seq'::regclass)"
            ),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("contragent_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ["contragent_id"],
            ["contragents.id"],
            name="marketplace_contragent_cart_contragent_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="marketplace_contragent_cart_pkey"),
    )
    op.create_index(
        "marketplace_contragent_cart_contragent_id_key",
        "marketplace_contragent_cart",
        ["contragent_id"],
        unique=True,
    )
    op.create_index(
        "ix_marketplace_contragent_cart_id",
        "marketplace_contragent_cart",
        ["id"],
        unique=False,
    )

    op.drop_constraint(
        "marketplace_cart_goods_cart_id_fkey",
        "marketplace_cart_goods",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "marketplace_cart_goods_cart_id_fkey",
        "marketplace_cart_goods",
        "marketplace_contragent_cart",
        ["cart_id"],
        ["id"],
    )
    op.drop_index(
        op.f("ix_marketplace_searches_phone"), table_name="marketplace_searches"
    )
    op.drop_index(op.f("ix_marketplace_searches_id"), table_name="marketplace_searches")
    op.drop_table("marketplace_searches")
    op.drop_index(op.f("ix_marketplace_orders_phone"), table_name="marketplace_orders")
    op.drop_index(op.f("ix_marketplace_orders_id"), table_name="marketplace_orders")
    op.drop_table("marketplace_orders")
    op.drop_index(
        op.f("ix_marketplace_favorites_phone"), table_name="marketplace_favorites"
    )
    op.drop_index(
        op.f("ix_marketplace_favorites_id"), table_name="marketplace_favorites"
    )
    op.drop_table("marketplace_favorites")
    op.drop_index(op.f("ix_marketplace_carts_phone"), table_name="marketplace_carts")
    op.drop_index(op.f("ix_marketplace_carts_id"), table_name="marketplace_carts")
    op.drop_table("marketplace_carts")
    op.drop_index(
        op.f("ix_marketplace_clients_list_phone"), table_name="marketplace_clients_list"
    )
    op.drop_index(
        op.f("ix_marketplace_clients_list_id"), table_name="marketplace_clients_list"
    )
    op.drop_table("marketplace_clients_list")
    op.create_table(
        "favorites_nomenclatures",
        sa.Column(
            "id",
            sa.INTEGER(),
            server_default=sa.text(
                "nextval('favorites_nomenclatures_id_seq'::regclass)"
            ),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("nomenclature_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column("contagent_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["contagent_id"],
            ["contragents.id"],
            name="favorites_nomenclatures_contagent_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["nomenclature_id"],
            ["nomenclature.id"],
            name="favorites_nomenclatures_nomenclature_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="favorites_nomenclatures_pkey"),
    )
    op.create_index(
        "ix_favorites_nomenclatures_id",
        "favorites_nomenclatures",
        ["id"],
        unique=False,
    )
