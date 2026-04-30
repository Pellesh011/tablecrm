"""optimize marketplace indexes
Revision ID: optimize_marketplace_001
Revises: d37339fcefc8
Create Date: 2026-01-02 14:00:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "optimize_marketplace_001"
down_revision = "d37339fcefc8"  # Изменено: указываем на последнюю миграцию в dev ветке
branch_labels = None
depends_on = None


def upgrade():
    """
    Создает индексы для оптимизации запросов маркетплейса.
    Оптимизирует:
    1. warehouse_balances - для быстрого получения последних остатков
    2. prices - для быстрого получения актуальных цен
    """
    # 1. Составной индекс для warehouse_balances
    # Оптимизирует GROUP BY (organization_id, warehouse_id, nomenclature_id) и MAX(id)
    # Используется в подзапросе для получения последних остатков
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_warehouse_balances_org_wh_nom_id_desc
        ON warehouse_balances (organization_id, warehouse_id, nomenclature_id, id DESC)
        WHERE organization_id IS NOT NULL
          AND warehouse_id IS NOT NULL
          AND nomenclature_id IS NOT NULL
        """
    )
    # 2. Индекс для prices по price_type и is_deleted
    # Оптимизирует фильтрацию по типу цены "chatting" и неудаленным ценам
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_type_deleted_nom_id_desc
        ON prices (price_type, is_deleted, nomenclature, id DESC)
        WHERE is_deleted IS NOT TRUE
        """
    )
    # 3. Дополнительный индекс для prices по датам (для оптимизации сортировки)
    # Помогает при сортировке по date_from и date_to
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_dates_created_id_desc
        ON prices (nomenclature, date_from, date_to, created_at DESC, id DESC)
        WHERE is_deleted IS NOT TRUE
        """
    )
    # 4. Индекс для docs_sales_goods по nomenclature (для подсчета total_sold)
    # Улучшает производительность GROUP BY nomenclature
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_docs_sales_goods_nomenclature
        ON docs_sales_goods (nomenclature)
        WHERE nomenclature IS NOT NULL
        """
    )
    # 5. Индексы на внешние ключи для оптимизации JOIN'ов
    # Ускоряют JOIN операции в основном запросе
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nomenclature_unit
        ON nomenclature (unit)
        WHERE unit IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nomenclature_category
        ON nomenclature (category)
        WHERE category IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nomenclature_manufacturer
        ON nomenclature (manufacturer)
        WHERE manufacturer IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nomenclature_cashbox
        ON nomenclature (cashbox)
        WHERE cashbox IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pictures_entity_id
        ON pictures (entity, entity_id)
        WHERE entity = 'nomenclature' AND is_deleted IS NOT TRUE
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nomenclature_barcodes_nomenclature_id
        ON nomenclature_barcodes (nomenclature_id)
        WHERE nomenclature_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_marketplace_rating_entity
        ON marketplace_rating_aggregates (entity_type, entity_id)
        WHERE entity_type = 'nomenclature'
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cboxes_admin
        ON cashboxes (admin)
        WHERE admin IS NOT NULL
        """
    )


def downgrade():
    """Удаляет созданные индексы"""
    op.execute("DROP INDEX IF EXISTS idx_warehouse_balances_org_wh_nom_id_desc")
    op.execute("DROP INDEX IF EXISTS idx_prices_type_deleted_nom_id_desc")
    op.execute("DROP INDEX IF EXISTS idx_prices_dates_created_id_desc")
    op.execute("DROP INDEX IF EXISTS idx_docs_sales_goods_nomenclature")
    op.execute("DROP INDEX IF EXISTS idx_nomenclature_unit")
    op.execute("DROP INDEX IF EXISTS idx_nomenclature_category")
    op.execute("DROP INDEX IF EXISTS idx_nomenclature_manufacturer")
    op.execute("DROP INDEX IF EXISTS idx_nomenclature_cashbox")
    op.execute("DROP INDEX IF EXISTS idx_pictures_entity_id")
    op.execute("DROP INDEX IF EXISTS idx_nomenclature_barcodes_nomenclature_id")
    op.execute("DROP INDEX IF EXISTS idx_marketplace_rating_entity")
    op.execute("DROP INDEX IF EXISTS idx_cboxes_admin")
