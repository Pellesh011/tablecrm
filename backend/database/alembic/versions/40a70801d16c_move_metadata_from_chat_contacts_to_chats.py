"""move metadata from chat_contacts to chats

Revision ID: 40a70801d16c
Revises: 6c67423f48e5
Create Date: 2025-12-02 20:30:00.000000

"""

import json

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "40a70801d16c"
down_revision = "6c67423f48e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    # Убедимся, что metadata есть в chats (на всякий случай)
    chats_columns = [col["name"] for col in inspector.get_columns("chats")]
    if "metadata" not in chats_columns:
        op.add_column("chats", sa.Column("metadata", postgresql.JSONB(), nullable=True))

    # Проверяем, существует ли колонка metadata в chat_contacts
    chat_contacts_columns = [
        col["name"] for col in inspector.get_columns("chat_contacts")
    ]
    if "metadata" not in chat_contacts_columns:
        # Колонки нет — переносить нечего. Выходим тихо.
        return

    # Только если колонка есть — переносим
    if (
        "chats" in inspector.get_table_names()
        and "chat_contacts" in inspector.get_table_names()
    ):
        chats_with_contacts = conn.execute(
            sa.text(
                """
                SELECT c.id, cc.metadata
                FROM chats c
                JOIN chat_contacts cc ON c.chat_contact_id = cc.id
                WHERE cc.metadata IS NOT NULL
            """
            )
        ).fetchall()

        for chat_id, contact_metadata in chats_with_contacts:
            if contact_metadata:
                # Передаём напрямую — SQLAlchemy сам обработает JSONB
                conn.execute(
                    sa.text("UPDATE chats SET metadata = :meta WHERE id = :chat_id"),
                    {"meta": contact_metadata, "chat_id": chat_id},
                )

    # Удаляем колонку, если она ещё есть
    if "metadata" in chat_contacts_columns:
        op.drop_column("chat_contacts", "metadata")


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    # Восстанавливаем поле metadata в chat_contacts
    chat_contacts_columns = [
        col["name"] for col in inspector.get_columns("chat_contacts")
    ]
    if "metadata" not in chat_contacts_columns:
        op.add_column(
            "chat_contacts",
            sa.Column(
                "metadata", postgresql.JSON(astext_type=sa.Text()), nullable=True
            ),
        )

    # Переносим метадату обратно из chats в chat_contacts
    if (
        "chats" in inspector.get_table_names()
        and "chat_contacts" in inspector.get_table_names()
    ):
        chats_with_metadata = conn.execute(
            sa.text(
                """
            SELECT c.id, c.metadata, c.chat_contact_id
            FROM chats c
            WHERE c.metadata IS NOT NULL AND c.chat_contact_id IS NOT NULL
        """
            )
        ).fetchall()

        for chat_row in chats_with_metadata:
            chat_id, chat_metadata, contact_id = chat_row
            if chat_metadata:
                # Обновляем метадату в контакте
                # Преобразуем dict в JSON строку для PostgreSQL
                if isinstance(chat_metadata, dict):
                    metadata_json = json.dumps(chat_metadata)
                else:
                    metadata_json = chat_metadata
                # Используем cast для преобразования в jsonb
                conn.execute(
                    sa.text(
                        """
                    UPDATE chat_contacts
                    SET metadata = CAST(:metadata AS jsonb)
                    WHERE id = :contact_id
                """
                    ),
                    {"metadata": metadata_json, "contact_id": contact_id},
                )

    # Удаляем поле metadata из chats (если оно было добавлено в этой миграции)
    chats_columns = [col["name"] for col in inspector.get_columns("chats")]
    if "metadata" in chats_columns:
        op.drop_column("chats", "metadata")
