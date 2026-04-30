"""remove passiflora duplicates

Revision ID: 76d4216e5d87
Revises: merge_tariff_passiflora
Create Date: 2026-01-31 22:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "76d4216e5d87"
down_revision = "channels_name_active"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Удаляет все price_type с name='passiflora'.

    Причина: миграция c4e8f1d0a9b2 создала дубликаты passiflora
    (по одному для каждого relation_tg_cashboxes вместо одного на cashbox).

    После удаления, passiflora будет создаваться автоматически
    при первой синхронизации цен через PoziToTableSyncPricesHandler.
    """
    op.execute(
        """
        DELETE FROM price_types
        WHERE name = 'passiflora';
        """
    )


def downgrade() -> None:
    """
    Downgrade не восстанавливает passiflora, так как это исправление бага.
    passiflora будет создан автоматически при синхронизации цен.
    """
    pass
