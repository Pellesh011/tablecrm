"""add has_products to global_categories

Revision ID: a1b2c3d4e5f6
Revises: optimize_marketplace_001
Create Date: 2026-01-16 10:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "8744e1ee787b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем колонку has_products в таблицу global_categories
    op.add_column(
        "global_categories",
        sa.Column("has_products", sa.Boolean(), nullable=True, server_default="false"),
    )

    # Создаем индекс для быстрой фильтрации
    op.create_index(
        op.f("ix_global_categories_has_products"),
        "global_categories",
        ["has_products"],
        unique=False,
    )

    # Инициализируем значение has_products для существующих категорий.
    # Если колонка global_category_id в таблице nomenclature по каким-то причинам
    # ещё не существует (несогласованность состояний миграций между ветками),
    # просто пропускаем инициализацию, оставляя значение по умолчанию.
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    nomenclature_columns = [
        col["name"] for col in inspector.get_columns("nomenclature")
    ]

    if "global_category_id" in nomenclature_columns:
        op.execute(
            """
            UPDATE global_categories gc
            SET has_products = EXISTS(
                SELECT 1
                FROM nomenclature n
                WHERE n.global_category_id = gc.id
                  AND n.is_deleted IS NOT TRUE
            )
        """
        )


def downgrade() -> None:
    # Удаляем индекс
    op.drop_index(
        op.f("ix_global_categories_has_products"), table_name="global_categories"
    )

    # Удаляем колонку
    op.drop_column("global_categories", "has_products")
