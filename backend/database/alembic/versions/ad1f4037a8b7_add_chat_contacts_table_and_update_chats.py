"""add_chat_contacts_table_and_update_chats

Revision ID: ad1f4037a8b7
Revises: 96f6db5dad07
Create Date: 2025-11-26 04:42:50.511052

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "ad1f4037a8b7"
down_revision = "96f6db5dad07"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = inspector.get_table_names()

    # 1. Создаем таблицу chat_contacts
    if "chat_contacts" not in tables:
        op.create_table(
            "chat_contacts",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("channel_id", sa.Integer(), nullable=False),
            sa.Column("external_contact_id", sa.String(length=255), nullable=True),
            sa.Column("name", sa.String(length=100), nullable=True),
            sa.Column("phone", sa.String(length=20), nullable=True),
            sa.Column("email", sa.String(length=255), nullable=True),
            sa.Column("avatar", sa.String(length=500), nullable=True),
            sa.Column("contragent_id", sa.Integer(), nullable=True),
            sa.Column(
                "metadata", postgresql.JSON(astext_type=sa.Text()), nullable=True
            ),
            sa.Column(
                "created_at",
                sa.DateTime(),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.ForeignKeyConstraint(
                ["channel_id"], ["channels.id"], ondelete="CASCADE"
            ),
            sa.ForeignKeyConstraint(
                ["contragent_id"], ["contragents.id"], ondelete="SET NULL"
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "channel_id",
                "external_contact_id",
                name="uq_chat_contacts_channel_external",
            ),
        )
        op.create_index(
            "ix_chat_contacts_channel_id", "chat_contacts", ["channel_id"], unique=False
        )
        op.create_index(
            "ix_chat_contacts_contragent_id",
            "chat_contacts",
            ["contragent_id"],
            unique=False,
        )

    # 2. Добавляем chat_contact_id в chats (пока nullable)
    columns = [col["name"] for col in inspector.get_columns("chats")]
    if "chat_contact_id" not in columns:
        op.add_column(
            "chats", sa.Column("chat_contact_id", sa.Integer(), nullable=True)
        )
        op.create_foreign_key(
            "fk_chats_chat_contact_id",
            "chats",
            "chat_contacts",
            ["chat_contact_id"],
            ["id"],
            ondelete="SET NULL",
        )
        op.create_index(
            "ix_chats_chat_contact_id", "chats", ["chat_contact_id"], unique=False
        )

    # 3. Мигрируем данные из chats в chat_contacts
    # Для каждого чата создаем или находим chat_contact
    if "chats" in tables and "chat_contacts" in tables:
        # Получаем все чаты с данными
        chats_data = conn.execute(
            sa.text(
                """
            SELECT id, channel_id, external_chat_id, phone, name, contragent_id
            FROM chats
            WHERE phone IS NOT NULL OR name IS NOT NULL
        """
            )
        ).fetchall()

        for chat_row in chats_data:
            chat_id, channel_id, external_chat_id, phone, name, contragent_id = chat_row

            # Ищем существующий chat_contact по channel_id и external_contact_id
            existing_contact = conn.execute(
                sa.text(
                    """
                SELECT id FROM chat_contacts
                WHERE channel_id = :channel_id AND external_contact_id = :external_contact_id
            """
                ),
                {"channel_id": channel_id, "external_contact_id": external_chat_id},
            ).fetchone()

            if existing_contact:
                contact_id = existing_contact[0]
                # Обновляем существующий контакт, если есть новые данные
                conn.execute(
                    sa.text(
                        """
                    UPDATE chat_contacts
                    SET name = COALESCE(:name, name),
                        phone = COALESCE(:phone, phone),
                        contragent_id = COALESCE(:contragent_id, contragent_id),
                        updated_at = now()
                    WHERE id = :contact_id
                """
                    ),
                    {
                        "name": name,
                        "phone": phone,
                        "contragent_id": contragent_id,
                        "contact_id": contact_id,
                    },
                )
            else:
                # Создаем новый chat_contact
                result = conn.execute(
                    sa.text(
                        """
                    INSERT INTO chat_contacts (channel_id, external_contact_id, name, phone, contragent_id, created_at, updated_at)
                    VALUES (:channel_id, :external_contact_id, :name, :phone, :contragent_id, now(), now())
                    RETURNING id
                """
                    ),
                    {
                        "channel_id": channel_id,
                        "external_contact_id": external_chat_id,
                        "name": name,
                        "phone": phone,
                        "contragent_id": contragent_id,
                    },
                )
                contact_id = result.fetchone()[0]

            # Связываем чат с chat_contact
            conn.execute(
                sa.text(
                    """
                UPDATE chats
                SET chat_contact_id = :contact_id
                WHERE id = :chat_id
            """
                ),
                {"contact_id": contact_id, "chat_id": chat_id},
            )

        conn.commit()

    # 4. Удаляем старые поля из chats
    columns = [col["name"] for col in inspector.get_columns("chats")]

    # Удаляем foreign key constraint для contragent_id перед удалением колонки
    if "contragent_id" in columns:
        # Проверяем существующие foreign keys
        fk_constraints = inspector.get_foreign_keys("chats")
        for fk in fk_constraints:
            if "contragent_id" in fk["constrained_columns"]:
                op.drop_constraint(fk["name"], "chats", type_="foreignkey")
        op.drop_column("chats", "contragent_id")

    if "phone" in columns:
        op.drop_column("chats", "phone")

    if "name" in columns:
        op.drop_column("chats", "name")


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("chats")]
    tables = inspector.get_table_names()

    # 1. Восстанавливаем поля в chats
    if "phone" not in columns:
        op.add_column("chats", sa.Column("phone", sa.String(length=20), nullable=True))

    if "name" not in columns:
        op.add_column("chats", sa.Column("name", sa.String(length=100), nullable=True))

    if "contragent_id" not in columns:
        op.add_column("chats", sa.Column("contragent_id", sa.Integer(), nullable=True))
        op.create_foreign_key(
            "fk_chats_contragent_id",
            "chats",
            "contragents",
            ["contragent_id"],
            ["id"],
            ondelete="SET NULL",
        )

    # 2. Восстанавливаем данные из chat_contacts в chats
    if "chat_contacts" in tables and "chats" in tables:
        chats_with_contacts = conn.execute(
            sa.text(
                """
            SELECT c.id, cc.name, cc.phone, cc.contragent_id
            FROM chats c
            JOIN chat_contacts cc ON c.chat_contact_id = cc.id
        """
            )
        ).fetchall()

        for chat_row in chats_with_contacts:
            chat_id, name, phone, contragent_id = chat_row
            conn.execute(
                sa.text(
                    """
                UPDATE chats
                SET name = :name, phone = :phone, contragent_id = :contragent_id
                WHERE id = :chat_id
            """
                ),
                {
                    "name": name,
                    "phone": phone,
                    "contragent_id": contragent_id,
                    "chat_id": chat_id,
                },
            )

        conn.commit()

    # 3. Удаляем chat_contact_id из chats
    columns = [col["name"] for col in inspector.get_columns("chats")]
    if "chat_contact_id" in columns:
        # Удаляем foreign key constraint
        fk_constraints = inspector.get_foreign_keys("chats")
        for fk in fk_constraints:
            if "chat_contact_id" in fk["constrained_columns"]:
                op.drop_constraint(fk["name"], "chats", type_="foreignkey")
        op.drop_index("ix_chats_chat_contact_id", table_name="chats")
        op.drop_column("chats", "chat_contact_id")

    # 4. Удаляем таблицу chat_contacts
    if "chat_contacts" in tables:
        op.drop_index("ix_chat_contacts_contragent_id", table_name="chat_contacts")
        op.drop_index("ix_chat_contacts_channel_id", table_name="chat_contacts")
        op.drop_table("chat_contacts")
