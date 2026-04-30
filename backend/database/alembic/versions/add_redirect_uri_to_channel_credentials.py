"""add_redirect_uri_to_channel_credentials

Revision ID: add_redirect_uri_cc
Revises: 96f6db5dad07
Create Date: 2025-12-21 15:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_redirect_uri_cc"
down_revision = "96f6db5dad07"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем поле redirect_uri для хранения настраиваемого redirect URI для OAuth
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("channel_credentials")]

    if "redirect_uri" not in columns:
        op.add_column(
            "channel_credentials",
            sa.Column("redirect_uri", sa.String(length=500), nullable=True),
        )


def downgrade() -> None:
    # Удаляем поле redirect_uri
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("channel_credentials")]

    if "redirect_uri" in columns:
        op.drop_column("channel_credentials", "redirect_uri")
