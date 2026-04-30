"""Add composite indexes on marketplace_orders for orders list and last address queries.

Revision ID: add_mp_orders_composite_idx
Revises: 93828b93a002
Create Date: 2026-03-12
"""

from alembic import op

revision = "add_mp_orders_composite_idx"
down_revision = "93828b93a002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mp_orders_client_id_created_at_desc
        ON marketplace_orders (client_id, created_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mp_orders_client_id_created_at_delivery
        ON marketplace_orders (client_id, created_at DESC)
        INCLUDE (delivery_info)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_mp_orders_client_id_created_at_desc")
    op.execute("DROP INDEX IF EXISTS idx_mp_orders_client_id_created_at_delivery")
