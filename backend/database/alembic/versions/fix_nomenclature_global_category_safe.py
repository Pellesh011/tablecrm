"""Ensure nomenclature.global_category_id column exists

Revision ID: fix_nomenclature_global_category_safe
Revises: alembic_version_widen
Create Date: 2026-01-18 00:15:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fix_nomenclature_global_category_safe"
down_revision = "alembic_version_widen"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add nomenclature.global_category_id if it is missing.

    Колонка активно используется в коде и моделях, но из-за особенностей
    применения миграций могла отсутствовать в БД. Добавляем её и индекс
    только при отсутствии, чтобы не дублировать структуру.
    """

    conn = op.get_bind()
    inspector = sa.inspect(conn)

    columns = [col["name"] for col in inspector.get_columns("nomenclature")]

    if "global_category_id" not in columns:
        op.add_column(
            "nomenclature",
            sa.Column("global_category_id", sa.Integer(), nullable=True),
        )

    # Добавляем индекс, если его нет
    index_names = {ix["name"] for ix in inspector.get_indexes("nomenclature")}

    if "ix_nomenclature_global_category_id" not in index_names:
        op.create_index(
            "ix_nomenclature_global_category_id",
            "nomenclature",
            ["global_category_id"],
            unique=False,
        )


def downgrade() -> None:
    # Аккуратный откат: сначала индекс (если есть), потом колонка.
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    index_names = {ix["name"] for ix in inspector.get_indexes("nomenclature")}
    if "ix_nomenclature_global_category_id" in index_names:
        op.drop_index("ix_nomenclature_global_category_id", table_name="nomenclature")

    columns = [col["name"] for col in inspector.get_columns("nomenclature")]
    if "global_category_id" in columns:
        op.drop_column("nomenclature", "global_category_id")
