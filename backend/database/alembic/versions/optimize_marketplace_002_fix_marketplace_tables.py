"""Ensure marketplace tables exist

Revision ID: optimize_marketplace_002
Revises: a1b2c3d4e5f6
Create Date: 2026-01-18 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "optimize_marketplace_002"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create marketplace tables if they are missing.

    На некоторых базах миграция reworking marketplace models (16d87642e03c)
    числится применённой, но сами таблицы могли быть удалены вручную либо
    база могла быть пересоздана без повторного прогона миграций.

    Чтобы привести схему к ожидаемому состоянию, создаём нужные таблицы
    и индексы только если их ещё нет.
    """

    conn = op.get_bind()
    inspector = sa.inspect(conn)

    def has_table(name: str) -> bool:
        return inspector.has_table(name)

    # marketplace_clients_list
    if not has_table("marketplace_clients_list"):
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

    # marketplace_carts
    if not has_table("marketplace_carts"):
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
            op.f("ix_marketplace_carts_id"),
            "marketplace_carts",
            ["id"],
            unique=False,
        )
        op.create_index(
            op.f("ix_marketplace_carts_phone"),
            "marketplace_carts",
            ["phone"],
            unique=True,
        )

    # marketplace_favorites
    if not has_table("marketplace_favorites"):
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

    # marketplace_orders
    if not has_table("marketplace_orders"):
        op.create_table(
            "marketplace_orders",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("phone", sa.String(), nullable=False),
            sa.Column(
                "status",
                sa.String(length=32),
                server_default="created",
                nullable=False,
            ),
            sa.Column(
                "delivery_info",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
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
            op.f("ix_marketplace_orders_id"),
            "marketplace_orders",
            ["id"],
            unique=False,
        )
        op.create_index(
            op.f("ix_marketplace_orders_phone"),
            "marketplace_orders",
            ["phone"],
            unique=False,
        )

    # marketplace_searches
    if not has_table("marketplace_searches"):
        op.create_table(
            "marketplace_searches",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("phone", sa.String(), nullable=True),
            sa.Column("query", sa.Text(), nullable=False),
            sa.Column(
                "filters",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            ),
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
            op.f("ix_marketplace_searches_id"),
            "marketplace_searches",
            ["id"],
            unique=False,
        )
        op.create_index(
            op.f("ix_marketplace_searches_phone"),
            "marketplace_searches",
            ["phone"],
            unique=False,
        )


def downgrade() -> None:
    # Откат намеренно не реализован, чтобы не трогать уже существующие
    # в проде таблицы. При необходимости изменения схемы будут сделаны
    # отдельными миграциями.
    pass
