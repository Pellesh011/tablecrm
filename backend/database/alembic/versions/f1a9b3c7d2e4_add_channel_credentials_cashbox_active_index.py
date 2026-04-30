"""Add channel credentials cashbox/is_active index.

Revision ID: f1a9b3c7d2e4
Revises: c2d4e6f8a9b0
Create Date: 2026-04-07
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f1a9b3c7d2e4"
down_revision = "c2d4e6f8a9b0"
branch_labels = None
depends_on = None


def _create_index_concurrently_if_not_exists(index_name: str, ddl: str) -> None:
    conn = op.get_bind()
    exists = conn.scalar(
        sa.text("SELECT to_regclass(:name)"),
        {"name": f"public.{index_name}"},
    )
    if exists is None:
        with op.get_context().autocommit_block():
            op.execute(ddl)


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = set(inspector.get_table_names())

    if "channel_credentials" in tables:
        _create_index_concurrently_if_not_exists(
            "ix_channel_credentials_cashbox_active",
            """
            CREATE INDEX CONCURRENTLY ix_channel_credentials_cashbox_active
            ON channel_credentials (cashbox_id, is_active)
            """,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS ix_channel_credentials_cashbox_active"
        )
