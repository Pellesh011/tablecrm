"""add_auto_sync_chats_enabled_to_channel_credentials

Revision ID: 96f6db5dad07
Revises: 36961ca3ffa1
Create Date: 2025-11-26 04:23:41.011005

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "96f6db5dad07"
down_revision = "36961ca3ffa1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем поле auto_sync_chats_enabled для включения/выключения автоматической выгрузки чатов
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("channel_credentials")]

    if "auto_sync_chats_enabled" not in columns:
        op.add_column(
            "channel_credentials",
            sa.Column(
                "auto_sync_chats_enabled",
                sa.Boolean(),
                nullable=False,
                server_default="false",
            ),
        )


def downgrade() -> None:
    # Удаляем поле auto_sync_chats_enabled
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("channel_credentials")]

    if "auto_sync_chats_enabled" in columns:
        op.drop_column("channel_credentials", "auto_sync_chats_enabled")
