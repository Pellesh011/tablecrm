"""add docs_purchases_id to payments

Revision ID: 5633b0a50e72
Revises: 0053a68ec9ec
Create Date: 2025-11-05 07:07:44.735181

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5633b0a50e72"
down_revision = "0053a68ec9ec"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column(
            "docs_purchases_id", sa.Integer(), sa.ForeignKey("docs_purchases.id")
        ),
    )


def downgrade() -> None:
    op.drop_column("payments", "docs_purchases_id")
