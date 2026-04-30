"""rework chats table

Revision ID: e8cbfae7d3e4
Revises: f14e8e594233
Create Date: 2026-04-15 02:38:11.480181

"""

import sqlalchemy as sa
from alembic import op

revision = "e8cbfae7d3e4"
down_revision = "f14e8e594233"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    channels_cols = {c["name"] for c in inspector.get_columns("channels")}
    if "cashbox_id" not in channels_cols:
        op.add_column(
            "channels",
            sa.Column(
                "cashbox_id",
                sa.Integer(),
                sa.ForeignKey("cashboxes.id", ondelete="CASCADE"),
                nullable=True,
            ),
        )

        op.execute(
            """
            UPDATE channels c
            SET cashbox_id = (
                SELECT cc.cashbox_id
                FROM channel_credentials cc
                WHERE cc.channel_id = c.id
                  AND cc.is_active = TRUE
                ORDER BY cc.id
                LIMIT 1
            )
            WHERE c.cashbox_id IS NULL
            """
        )

        op.create_index(
            "ix_channels_cashbox_id",
            "channels",
            ["cashbox_id"],
        )

        try:
            op.drop_index("uq_channels_name_when_active", table_name="channels")
        except Exception:
            pass

        op.create_index(
            "uq_channels_cashbox_name_active",
            "channels",
            ["cashbox_id", "name"],
            unique=True,
            postgresql_where=sa.text("is_active IS TRUE"),
        )

    contacts_cols = {c["name"] for c in inspector.get_columns("chat_contacts")}

    if "channel_id" in contacts_cols:

        fks = inspector.get_foreign_keys("chat_contacts")
        for fk in fks:
            if "channel_id" in fk.get("constrained_columns", []):
                op.drop_constraint(fk["name"], "chat_contacts", type_="foreignkey")

        unique_constraints = inspector.get_unique_constraints("chat_contacts")
        for uc in unique_constraints:
            if "channel_id" in uc.get("column_names", []):
                op.drop_constraint(uc["name"], "chat_contacts", type_="unique")

        indexes = inspector.get_indexes("chat_contacts")
        for idx in indexes:
            if "channel_id" in idx.get("column_names", []):
                op.drop_index(idx["name"], table_name="chat_contacts")

        op.drop_column("chat_contacts", "channel_id")

    contacts_indexes = {i["name"] for i in inspector.get_indexes("chat_contacts")}
    if "ix_chat_contacts_cashbox_id" not in contacts_indexes:
        op.create_index(
            "ix_chat_contacts_cashbox_id",
            "chat_contacts",
            ["cashbox_id"],
        )

    existing_tables = inspector.get_table_names()
    if "chat_contact_links" not in existing_tables:
        op.create_table(
            "chat_contact_links",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "chat_id",
                sa.Integer(),
                sa.ForeignKey("chats.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "contact_id",
                sa.Integer(),
                sa.ForeignKey("chat_contacts.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "role",
                sa.String(32),
                nullable=False,
                server_default="participant",
            ),
            sa.Column(
                "created_at",
                sa.DateTime(),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.UniqueConstraint(
                "chat_id",
                "contact_id",
                name="uq_chat_contact_links_chat_contact",
            ),
        )
        op.create_index(
            "ix_chat_contact_links_chat_id",
            "chat_contact_links",
            ["chat_id"],
        )
        op.create_index(
            "ix_chat_contact_links_contact_id",
            "chat_contact_links",
            ["contact_id"],
        )

        op.execute(
            """
            INSERT INTO chat_contact_links (chat_id, contact_id, role, created_at)
            SELECT id, chat_contact_id, 'participant', NOW()
            FROM chats
            WHERE chat_contact_id IS NOT NULL
            ON CONFLICT (chat_id, contact_id) DO NOTHING
            """
        )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    existing_tables = inspector.get_table_names()
    if "chat_contact_links" in existing_tables:
        op.drop_table("chat_contact_links")

    contacts_cols = {c["name"] for c in inspector.get_columns("chat_contacts")}
    if "channel_id" not in contacts_cols:
        op.add_column(
            "chat_contacts",
            sa.Column(
                "channel_id",
                sa.Integer(),
                sa.ForeignKey("channels.id", ondelete="CASCADE"),
                nullable=True,
            ),
        )

    channels_cols = {c["name"] for c in inspector.get_columns("channels")}
    if "cashbox_id" in channels_cols:
        try:
            op.drop_index("uq_channels_cashbox_name_active", table_name="channels")
        except Exception:
            pass
        try:
            op.drop_index("ix_channels_cashbox_id", table_name="channels")
        except Exception:
            pass

        op.create_index(
            "uq_channels_name_when_active",
            "channels",
            ["name"],
            unique=True,
            postgresql_where=sa.text("is_active IS TRUE"),
        )

        op.drop_column("channels", "cashbox_id")
