"""rename_referer_id_to_ref_user_in_marketplace_clients_list

Revision ID: 2a9ef18fb082
Revises: 4f9af03138e6
Create Date: 2026-01-19 13:59:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2a9ef18fb082"
down_revision = "4f9af03138e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Переименовываем referer_id в ref_user в marketplace_clients_list
    op.alter_column(
        "marketplace_clients_list",
        "referer_id",
        new_column_name="ref_user",
        existing_type=sa.String(),
        existing_nullable=True,
    )


def downgrade() -> None:
    # Возвращаем обратно ref_user в referer_id
    op.alter_column(
        "marketplace_clients_list",
        "ref_user",
        new_column_name="referer_id",
        existing_type=sa.String(),
        existing_nullable=True,
    )
