"""add tracking fields to docs_sales

Revision ID: add_docs_sales_tracking_fields
Revises: merge_heads_duplicates_76d4216e5d87
Create Date: 2026-02-01 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_docs_sales_tracking_fields"
down_revision = "merge_heads_duplicates_76d4216e5d87"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("docs_sales", sa.Column("track_number", sa.String(), nullable=True))
    op.add_column(
        "docs_sales", sa.Column("delivery_company", sa.String(), nullable=True)
    )
    op.add_column("docs_sales", sa.Column("order_source", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("docs_sales", "order_source")
    op.drop_column("docs_sales", "delivery_company")
    op.drop_column("docs_sales", "track_number")
