"""add indexes for docs_sales_calendar queries

Revision ID: 6d6ce46bbd44
Revises: f4d18b4db9a1
Create Date: 2025-08-22 21:09:12.156930

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6d6ce46bbd44"
down_revision = "f4d18b4db9a1"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()

    # 1) idx_docs_sales_cashbox_dated_id_not_deleted
    idx_name = "public.idx_docs_sales_cashbox_dated_id_not_deleted"
    exists = conn.scalar(sa.text("SELECT to_regclass(:n)"), {"n": idx_name})
    if exists is None:
        with op.get_context().autocommit_block():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY idx_docs_sales_cashbox_dated_id_not_deleted
                ON docs_sales (cashbox, dated, id DESC)
                WHERE is_deleted IS NOT TRUE;
            """
            )

    # 2) idx_docs_sales_delivery_info_delivery_date_docs_sales_id
    idx_name = "public.idx_docs_sales_delivery_info_delivery_date_docs_sales_id"
    exists = conn.scalar(sa.text("SELECT to_regclass(:n)"), {"n": idx_name})
    if exists is None:
        with op.get_context().autocommit_block():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY idx_docs_sales_delivery_info_delivery_date_docs_sales_id
                ON docs_sales_delivery_info (delivery_date, docs_sales_id);
            """
            )

    # 3) idx_docs_sales_id_cashbox_not_deleted
    idx_name = "public.idx_docs_sales_id_cashbox_not_deleted"
    exists = conn.scalar(sa.text("SELECT to_regclass(:n)"), {"n": idx_name})
    if exists is None:
        with op.get_context().autocommit_block():
            op.execute(
                """
                CREATE INDEX CONCURRENTLY idx_docs_sales_id_cashbox_not_deleted
                ON docs_sales (id, cashbox)
                WHERE is_deleted IS NOT TRUE;
            """
            )


def downgrade():
    # DROP INDEX CONCURRENTLY must also run outside transaction
    with op.get_context().autocommit_block():
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_cashbox_dated_id_not_deleted;"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_delivery_info_delivery_date_docs_sales_id;"
        )
        op.execute(
            "DROP INDEX CONCURRENTLY IF EXISTS idx_docs_sales_id_cashbox_not_deleted;"
        )
