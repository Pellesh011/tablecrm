"""add_new_statuses_for_order

Revision ID: f4d18b4db9a1
Revises: 6a8f802556c1
Create Date: 2025-08-18 01:02:11.244203

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f4d18b4db9a1"
down_revision = "6a8f802556c1"
branch_labels = None
depends_on = None


def upgrade():
    # Для PostgreSQL: добавляем новые значения в ENUM
    op.execute("ALTER TYPE orderstatus ADD VALUE 'success'")
    op.execute("ALTER TYPE orderstatus ADD VALUE 'closed'")


def downgrade():
    # Для PostgreSQL: сложная операция удаления значений из ENUM
    # Шаг 1: Обновляем существующие записи
    op.execute(
        "UPDATE docs_sales SET order_status = 'delivered' WHERE order_status = 'success'"
    )
    op.execute(
        "UPDATE docs_sales SET order_status = 'delivered' WHERE order_status = 'closed'"
    )

    # Шаг 2: Создаем временный тип
    op.execute(
        "CREATE TYPE orderstatus_temp AS ENUM('received','processed','collecting','collected','picked','delivered')"
    )

    # Шаг 3: Изменяем тип колонки
    op.execute(
        """
        ALTER TABLE docs_sales
        ALTER COLUMN order_status TYPE orderstatus_temp
        USING order_status::text::orderstatus_temp
    """
    )

    # Шаг 4: Удаляем старый тип и переименовываем временный
    op.execute("DROP TYPE orderstatus")
    op.execute("ALTER TYPE orderstatus_temp RENAME TO orderstatus")
