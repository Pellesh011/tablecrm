"""Add chat list performance indexes.

Revision ID: c2d4e6f8a9b0
Revises: 8f3b1e9c4d2a
Create Date: 2026-04-07
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c2d4e6f8a9b0"
down_revision = "8f3b1e9c4d2a"
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

    if "pictures" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_pictures_messages_entity_created",
            """
            CREATE INDEX CONCURRENTLY idx_pictures_messages_entity_created
            ON pictures (entity_id, created_at)
            WHERE entity = 'messages' AND is_deleted IS NOT TRUE
            """,
        )

    if "chats" in tables:
        _create_index_concurrently_if_not_exists(
            "idx_chats_cashbox_last_message",
            """
            CREATE INDEX CONCURRENTLY idx_chats_cashbox_last_message
            ON chats (cashbox_id, last_message_time DESC NULLS LAST)
            """,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_pictures_messages_entity_created"
        )
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_chats_cashbox_last_message")
