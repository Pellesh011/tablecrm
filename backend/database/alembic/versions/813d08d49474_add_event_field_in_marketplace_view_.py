"""add event field in marketplace_view_events

Revision ID: 813d08d49474
Revises: add_global_categories
Create Date: 2025-12-02 00:50:03.237679

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "813d08d49474"
down_revision = "add_global_categories"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "marketplace_view_events",
        sa.Column("event", sa.String(), nullable=False, server_default="view"),
    )


def downgrade() -> None:
    op.drop_column("marketplace_view_events", "event")
