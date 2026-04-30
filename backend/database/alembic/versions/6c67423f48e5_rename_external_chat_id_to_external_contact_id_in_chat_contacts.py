"""rename external_chat_id to external_contact_id in chat_contacts

Revision ID: 6c67423f48e5
Revises: eb6139bd2209
Create Date: 2025-12-02 16:50:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6c67423f48e5"
down_revision = "eb6139bd2209"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Переименовываем колонку external_chat_id обратно в external_contact_id в таблице chat_contacts
    op.alter_column(
        "chat_contacts",
        "external_chat_id",
        new_column_name="external_contact_id",
        existing_type=sa.String(length=255),
        existing_nullable=True,
    )

    # Переименовываем UniqueConstraint
    op.drop_constraint(
        "uq_chat_contacts_channel_external", "chat_contacts", type_="unique"
    )
    op.create_unique_constraint(
        "uq_chat_contacts_channel_external",
        "chat_contacts",
        ["channel_id", "external_contact_id"],
    )


def downgrade() -> None:
    # Возвращаем обратно
    op.drop_constraint(
        "uq_chat_contacts_channel_external", "chat_contacts", type_="unique"
    )
    op.create_unique_constraint(
        "uq_chat_contacts_channel_external",
        "chat_contacts",
        ["channel_id", "external_chat_id"],
    )

    op.alter_column(
        "chat_contacts",
        "external_contact_id",
        new_column_name="external_chat_id",
        existing_type=sa.String(length=255),
        existing_nullable=True,
    )
