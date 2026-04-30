"""add qr to chats

Revision ID: 02e7a1e8552a
Revises: c7966529d7af
Create Date: 2026-04-17 22:58:37.088586

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "02e7a1e8552a"
down_revision = "c7966529d7af"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "chats",
        sa.Column("is_qr", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("chats", "is_qr")
