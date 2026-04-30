"""add_status_fields_to_channel_credentials

Revision ID: 36961ca3ffa1
Revises: 86c2d7aba466
Create Date: 2025-11-26 04:15:08.574005

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "36961ca3ffa1"
down_revision = "86c2d7aba466"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем поля для хранения статуса проверки аккаунтов Avito
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("channel_credentials")]

    if "last_status_code" not in columns:
        op.add_column(
            "channel_credentials",
            sa.Column("last_status_code", sa.Integer(), nullable=True),
        )

    if "last_status_check_at" not in columns:
        op.add_column(
            "channel_credentials",
            sa.Column("last_status_check_at", sa.DateTime(), nullable=True),
        )

    if "connection_status" not in columns:
        op.add_column(
            "channel_credentials",
            sa.Column("connection_status", sa.String(length=50), nullable=True),
        )


def downgrade() -> None:
    # Удаляем поля статуса
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("channel_credentials")]

    if "connection_status" in columns:
        op.drop_column("channel_credentials", "connection_status")

    if "last_status_check_at" in columns:
        op.drop_column("channel_credentials", "last_status_check_at")

    if "last_status_code" in columns:
        op.drop_column("channel_credentials", "last_status_code")
