"""add column to tech oper

Revision ID: 911d5a30c5dc
Revises: 591308b65e11
Create Date: 2026-04-01 06:39:26.106683

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "911d5a30c5dc"
down_revision = "591308b65e11"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "tech_operations",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )


def downgrade():
    op.drop_column("tech_operations", "updated_at")
