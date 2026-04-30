"""add commerceml products_loaded_count and orders_exported_count

Revision ID: commerceml_counts_001
Revises: c15427691d19
Create Date: 2026-02-13

"""

import sqlalchemy as sa
from alembic import op

revision = "commerceml_counts_001"
down_revision = "c15427691d19"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "commerceml_connections",
        sa.Column(
            "products_loaded_count", sa.Integer(), nullable=False, server_default="0"
        ),
    )
    op.add_column(
        "commerceml_connections",
        sa.Column(
            "orders_exported_count", sa.Integer(), nullable=False, server_default="0"
        ),
    )


def downgrade() -> None:
    op.drop_column("commerceml_connections", "orders_exported_count")
    op.drop_column("commerceml_connections", "products_loaded_count")
