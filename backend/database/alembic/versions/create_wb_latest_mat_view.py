"""Create warehouse balances latest materialized view.

Revision ID: create_wb_latest_mat_view
Revises: optimize_marketplace_004
Create Date: 2026-03-16
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "create_wb_latest_mat_view"
down_revision = "optimize_marketplace_004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # materialized view для последних остатков по (организация, склад, номенклатура)
    op.execute(
        """
        CREATE MATERIALIZED VIEW warehouse_balances_latest AS
        SELECT DISTINCT ON (organization_id, warehouse_id, nomenclature_id)
            organization_id,
            warehouse_id,
            nomenclature_id,
            current_amount
        FROM warehouse_balances
        ORDER BY organization_id, warehouse_id, nomenclature_id, id DESC;
        """
    )
    # Уникальный индекс обязателен для CONCURRENTLY refresh
    op.execute(
        "CREATE UNIQUE INDEX ON warehouse_balances_latest (organization_id, warehouse_id, nomenclature_id)"
    )
    # Индекс для фильтрации/агрегаций по номенклатуре
    op.execute("CREATE INDEX ON warehouse_balances_latest (nomenclature_id)")


def downgrade() -> None:
    # Чистим view при откате миграции
    op.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse_balances_latest")
