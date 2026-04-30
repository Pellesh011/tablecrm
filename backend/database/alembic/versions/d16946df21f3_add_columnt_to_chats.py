"""add columnt to chats

Revision ID: d16946df21f3
Revises: 0d95a3178fdb
Create Date: 2026-04-13 22:38:47.962218

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d16946df21f3"
down_revision = "0d95a3178fdb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "chat_contacts", sa.Column("last_activity", sa.BigInteger(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("chat_contacts", "last_activity")
