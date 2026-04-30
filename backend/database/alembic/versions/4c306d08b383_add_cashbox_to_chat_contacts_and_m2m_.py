"""add_cashbox_to_chat_contacts_and_m2m_chat_contacts

Revision ID: 4c306d08b383
Revises: 74f03b582fcb
Create Date: 2026-04-08 06:01:03.536154
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "4c306d08b383"
down_revision = "74f03b582fcb"
branch_labels = None
depends_on = None


"""add_cashbox_to_chat_contacts_and_m2m_links_backward_compatible

Revision ID: fix_chat_contact_id
Revises: 74f03b582fcb
Create Date: 2026-04-08 12:30:00.000000
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "fix_chat_contact_id"
down_revision = "74f03b582fcb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)

    columns_cc = [c["name"] for c in inspector.get_columns("chat_contacts")]
    if "cashbox_id" not in columns_cc:
        op.add_column(
            "chat_contacts", sa.Column("cashbox_id", sa.Integer(), nullable=True)
        )
    else:
        print("Column 'cashbox_id' already exists, skipping.")
    op.execute(
        """
        UPDATE chat_contacts
        SET cashbox_id = (
            SELECT cashbox_id
            FROM chats
            WHERE chats.chat_contact_id = chat_contacts.id
            LIMIT 1
        )
        WHERE cashbox_id IS NULL
    """
    )

    op.alter_column("chat_contacts", "cashbox_id")

    if not inspector.has_table("chat_contact_links"):
        op.create_table(
            "chat_contact_links",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("chat_id", sa.Integer(), nullable=False),
            sa.Column("contact_id", sa.Integer(), nullable=False),
            sa.Column(
                "role", sa.String(32), server_default="participant", nullable=False
            ),
            sa.Column(
                "created_at",
                sa.DateTime(),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.ForeignKeyConstraint(["chat_id"], ["chats.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(
                ["contact_id"], ["chat_contacts.id"], ondelete="CASCADE"
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "chat_id", "contact_id", name="uq_chat_contact_links_chat_contact"
            ),
        )
        op.create_index(
            "ix_chat_contact_links_chat_id", "chat_contact_links", ["chat_id"]
        )
        op.create_index(
            "ix_chat_contact_links_contact_id", "chat_contact_links", ["contact_id"]
        )

    op.execute(
        """
        INSERT INTO chat_contact_links (chat_id, contact_id, role, created_at)
        SELECT id, chat_contact_id, 'participant', now()
        FROM chats
        WHERE chat_contact_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM chat_contact_links
            WHERE chat_contact_links.chat_id = chats.id
            AND chat_contact_links.contact_id = chats.chat_contact_id
        )
    """
    )
    try:
        op.create_foreign_key(
            "fk_chat_contacts_cashbox",
            "chat_contacts",
            "cashboxes",
            ["cashbox_id"],
            ["id"],
            ondelete="RESTRICT",
        )
    except Exception as e:
        print(f"FK already exists: {e}")


def downgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)

    try:
        op.drop_constraint(
            "fk_chat_contacts_cashbox", "chat_contacts", type_="foreignkey"
        )
    except Exception:
        pass

    if inspector.has_table("chat_contact_links"):
        op.drop_table("chat_contact_links")

    columns_cc = [c["name"] for c in inspector.get_columns("chat_contacts")]
    if "cashbox_id" in columns_cc:
        op.drop_column("chat_contacts", "cashbox_id")
