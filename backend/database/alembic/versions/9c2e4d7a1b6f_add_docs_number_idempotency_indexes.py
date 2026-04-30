"""add docs number idempotency indexes concurrently

Revision ID: 9c2e4d7a1b6f
Revises: fix_chat_contact_id
Create Date: 2026-04-13 19:30:00.000000
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9c2e4d7a1b6f"
down_revision = "fix_chat_contact_id"
branch_labels = None
depends_on = None


def _create_index_concurrently_if_not_exists(index_name: str, ddl: str) -> None:
    conn = op.get_bind()
    exists = conn.scalar(
        sa.text("SELECT to_regclass(:n)"), {"n": f"public.{index_name}"}
    )
    if exists is None:
        with op.get_context().autocommit_block():
            op.execute(ddl)


def upgrade() -> None:
    _create_index_concurrently_if_not_exists(
        "idx_docs_warehouse_cashbox_operation_number_active",
        """
        CREATE INDEX CONCURRENTLY idx_docs_warehouse_cashbox_operation_number_active
        ON docs_warehouse (cashbox, operation, number)
        WHERE is_deleted IS NOT TRUE AND number IS NOT NULL
        """,
    )

    _create_index_concurrently_if_not_exists(
        "idx_docs_purchases_cashbox_number_active",
        """
        CREATE INDEX CONCURRENTLY idx_docs_purchases_cashbox_number_active
        ON docs_purchases (cashbox, number)
        WHERE is_deleted IS NOT TRUE AND number IS NOT NULL
        """,
    )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_warehouse_cashbox_operation_number_active"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_purchases_cashbox_number_active"
        )
