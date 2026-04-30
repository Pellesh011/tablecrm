"""channels name unique only when active

Уникальность имени канала только среди активных каналов (is_active = true).
Неактивные каналы могут иметь совпадающие имена; переименование активного
канала в имя неактивного больше не блокируется.

Revision ID: channels_name_active
Revises: merge_tariff_passiflora
Create Date: 2025-01-28

"""

import sqlalchemy as sa
from alembic import op

revision = "channels_name_active"
down_revision = "merge_tariff_passiflora"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Убираем глобальный UNIQUE(name), вешаем частичный: уникальность только при is_active = true
    op.drop_constraint("channels_name_key", "channels", type_="unique")
    op.create_index(
        "uq_channels_name_when_active",
        "channels",
        ["name"],
        unique=True,
        postgresql_where=sa.text("is_active IS TRUE"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_channels_name_when_active",
        table_name="channels",
        postgresql_where=sa.text("is_active IS TRUE"),
    )
    op.create_unique_constraint("channels_name_key", "channels", ["name"])
