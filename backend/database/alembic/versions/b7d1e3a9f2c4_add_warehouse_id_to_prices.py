"""add warehouse_id to prices

Revision ID: b7d1e3a9f2c4
Revises: 67138537de70, ad1f4037a8b7
Create Date: 2026-01-31 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b7d1e3a9f2c4"
down_revision = ("67138537de70", "ad1f4037a8b7")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "prices",
        sa.Column(
            "warehouse_id", sa.Integer(), sa.ForeignKey("warehouses.id"), nullable=True
        ),
    )


def downgrade() -> None:
    op.drop_column("prices", "warehouse_id")
