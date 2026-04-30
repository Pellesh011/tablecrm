"""optimize segments 003 - add critical indexes

Revision ID: optimize_segments_003
Revises: optimize_segments_002
Create Date: 2025-10-09 19:33:08.204582

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "optimize_segments_003"
down_revision = "optimize_segments_002"
branch_labels = None
depends_on = None


def upgrade():
    """
    Creates critical indexes for segment query performance.
    Fast index creation without CONCURRENTLY.
    """

    # Enable pg_trgm extension if not already enabled (for ILIKE optimization)
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # Index for category name ILIKE searches (used heavily in segment filters)
    # This index already exists from optimize_segments_002, but ensuring it's there
    # op.execute(
    #     "CREATE INDEX IF NOT EXISTS idx_categories_name_trgm "
    #     "ON categories USING gin(name gin_trgm_ops)"
    # )

    # Composite index for docs_sales contragent aggregation queries
    # Helps with: GROUP BY contragent, COUNT(id), SUM(sum)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_docs_sales_contragent_cashbox_deleted "
        "ON docs_sales(contragent, cashbox) "
        "WHERE is_deleted IS NOT TRUE"
    )

    # Include index for sum aggregation on docs_sales
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_docs_sales_sum_include "
        "ON docs_sales(contragent) "
        "INCLUDE (id, sum, cashbox, created_at) "
        "WHERE is_deleted IS NOT TRUE"
    )

    # Index for loyalty cards with balance filtering
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_loyality_cards_contragent_balance_filtered "
        "ON loyality_cards(contragent_id, balance) "
        "WHERE contragent_id IS NOT NULL"
    )

    # Index for docs_sales_goods JOIN on nomenclature filtering
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_docs_sales_goods_nomenclature_id "
        "ON docs_sales_goods(docs_sales_id, nomenclature)"
    )

    # Index for nomenclature category filtering
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_nomenclature_category_id "
        "ON nomenclature(category, id)"
    )

    # Analyze tables after index creation
    op.execute("ANALYZE docs_sales")
    op.execute("ANALYZE loyality_cards")
    op.execute("ANALYZE docs_sales_goods")
    op.execute("ANALYZE nomenclature")
    op.execute("ANALYZE categories")


def downgrade():
    """Remove created indexes"""

    op.execute("DROP INDEX IF EXISTS idx_nomenclature_category_id")
    op.execute("DROP INDEX IF EXISTS idx_docs_sales_goods_nomenclature_id")
    op.execute("DROP INDEX IF EXISTS idx_loyality_cards_contragent_balance_filtered")
    op.execute("DROP INDEX IF EXISTS idx_docs_sales_sum_include")
    op.execute("DROP INDEX IF EXISTS idx_docs_sales_contragent_cashbox_deleted")
