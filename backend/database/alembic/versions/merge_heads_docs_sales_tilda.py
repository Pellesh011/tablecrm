"""merge heads for docs_sales and tilda feeds

Revision ID: merge_heads_docs_sales_tilda
Revises: add_docs_sales_tracking_fields, add_tilda_price_catalog_ids
Create Date: 2026-02-03 20:15:00.000000

"""

# revision identifiers, used by Alembic.
revision = "merge_heads_docs_sales_tilda"
down_revision = ("add_docs_sales_tracking_fields", "add_tilda_price_catalog_ids")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
