"""rename external_contact_id to external_chat_id

Revision ID: eb6139bd2209
Revises: ebd3d4fd346e
Create Date: 2025-12-02 02:20:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "eb6139bd2209"
down_revision = "ebd3d4fd346e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Переименовываем колонку external_contact_id в external_chat_id
    op.alter_column(
        "chat_contacts",
        "external_contact_id",
        new_column_name="external_chat_id",
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
        ["channel_id", "external_chat_id"],
    )


def downgrade() -> None:
    # Возвращаем обратно
    op.drop_constraint(
        "uq_chat_contacts_channel_external", "chat_contacts", type_="unique"
    )
    op.create_unique_constraint(
        "uq_chat_contacts_channel_external",
        "chat_contacts",
        ["channel_id", "external_contact_id"],
    )

    op.alter_column(
        "chat_contacts",
        "external_chat_id",
        new_column_name="external_contact_id",
        existing_type=sa.String(length=255),
        existing_nullable=True,
    )
