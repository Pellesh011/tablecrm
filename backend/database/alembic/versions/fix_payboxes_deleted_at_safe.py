"""Ensure payboxes.deleted_at column exists

Revision ID: fix_payboxes_deleted_at_safe
Revises: optimize_marketplace_002
Create Date: 2026-01-18 00:10:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fix_payboxes_deleted_at_safe"
down_revision = "optimize_marketplace_002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add payboxes.deleted_at if it is missing.

    Из-за изменения графа миграций Alembic колонка deleted_at могла не
    попасть в схему даже при том, что соответствующая миграция числится
    предком текущего head. Здесь мы проверяем наличие колонки и
    добавляем её только при отсутствии, чтобы привести схему к ожидаемой
    без дублирования.
    """

    conn = op.get_bind()
    inspector = sa.inspect(conn)

    columns = [col["name"] for col in inspector.get_columns("payboxes")]

    if "deleted_at" not in columns:
        op.add_column(
            "payboxes",
            sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    # Откат: убираем колонку только если она есть, чтобы не падать на
    # базах, где структура уже изменилась вручную.
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("payboxes")]

    if "deleted_at" in columns:
        op.drop_column("payboxes", "deleted_at")
