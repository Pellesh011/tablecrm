"""add segments deletion

Revision ID: ded27e5f734b
Revises: 25ce6f85de08
Create Date: 2025-08-12 02:33:18.752992

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ded27e5f734b"
down_revision = "25ce6f85de08"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "segments",
        sa.Column("is_deleted", sa.Boolean(), server_default="false", nullable=False),
    )

    op.execute("UPDATE segments SET is_deleted = false WHERE is_deleted IS NULL")


def downgrade() -> None:
    op.drop_column("segments", "is_deleted")
