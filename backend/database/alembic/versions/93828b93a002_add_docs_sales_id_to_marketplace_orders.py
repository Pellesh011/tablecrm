"""Add docs_sales_id to marketplace_orders.

Revision ID: 93828b93a002
Revises: fix_marketplace_clients_001
Create Date: 2026-03-12
"""

import sqlalchemy as sa
from alembic import op

revision = "93828b93a002"
down_revision = "fix_marketplace_clients_001"
branch_labels = None
depends_on = None


def _has_column(inspector, table_name: str, column_name: str) -> bool:
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _has_index(inspector, table_name: str, index_name: str) -> bool:
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _has_column(inspector, "marketplace_orders", "docs_sales_id"):
        op.add_column(
            "marketplace_orders",
            sa.Column(
                "docs_sales_id",
                sa.Integer(),
                sa.ForeignKey("docs_sales.id", ondelete="SET NULL"),
                nullable=True,
            ),
        )
    inspector = sa.inspect(bind)
    if not _has_index(
        inspector, "marketplace_orders", "ix_marketplace_orders_docs_sales_id"
    ):
        op.create_index(
            "ix_marketplace_orders_docs_sales_id",
            "marketplace_orders",
            ["docs_sales_id"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _has_index(
        inspector, "marketplace_orders", "ix_marketplace_orders_docs_sales_id"
    ):
        op.drop_index(
            "ix_marketplace_orders_docs_sales_id", table_name="marketplace_orders"
        )
    inspector = sa.inspect(bind)
    if _has_column(inspector, "marketplace_orders", "docs_sales_id"):
        op.drop_column("marketplace_orders", "docs_sales_id")
