"""rework qr chats

Revision ID: 56e509c694cf
Revises: 02e7a1e8552a
Create Date: 2026-04-18 22:42:47.533323

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "56e509c694cf"
down_revision = "02e7a1e8552a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем столбец qr_page_id (связь с таблицей qr_pages)
    op.add_column("chats", sa.Column("qr_page_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_chats_qr_page_id",
        "chats",
        "qr_pages",
        ["qr_page_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # Переименовываем is_qr → from_qr
    op.alter_column(
        "chats",
        "is_qr",
        new_column_name="from_qr",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.sql.false(),
    )


def downgrade() -> None:
    # Возвращаем имя столбца обратно
    op.alter_column(
        "chats",
        "from_qr",
        new_column_name="is_qr",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.sql.false(),
    )

    # Удаляем внешний ключ и столбец qr_page_id
    op.drop_constraint("fk_chats_qr_page_id", "chats", type_="foreignkey")
    op.drop_column("chats", "qr_page_id")
