"""add_rating_to_relation_tg_cashboxes

Revision ID: 32cf29284da2
Revises: af297d88e7fa
Create Date: 2025-11-11 20:59:38.657041

"""

import sqlalchemy as sa
from alembic import op

revision = "32cf29284da2"
down_revision = "a17a46ece83d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "relation_tg_cashboxes",
        sa.Column("rating", sa.Float(), nullable=True, server_default="0.0"),
    )


def downgrade() -> None:
    op.drop_column("relation_tg_cashboxes", "rating")
