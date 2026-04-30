"""add tilda price and catalog ids to feeds

Revision ID: add_tilda_price_catalog_ids
Revises: add_tilda_fields
Create Date: 2026-02-02 16:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_tilda_price_catalog_ids"
down_revision = "add_tilda_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем поля для ID цен и каталога Tilda
    op.add_column("feeds", sa.Column("tilda_price_id", sa.String(), nullable=True))
    op.add_column(
        "feeds", sa.Column("tilda_discount_price_id", sa.String(), nullable=True)
    )
    op.add_column("feeds", sa.Column("tilda_catalog_id", sa.String(), nullable=True))
    op.add_column("feeds", sa.Column("tilda_warehouse_id", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("feeds", "tilda_warehouse_id")
    op.drop_column("feeds", "tilda_catalog_id")
    op.drop_column("feeds", "tilda_discount_price_id")
    op.drop_column("feeds", "tilda_price_id")
