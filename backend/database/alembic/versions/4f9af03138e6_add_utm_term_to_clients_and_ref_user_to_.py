"""add_utm_term_to_clients_and_ref_user_to_utm_tags

Revision ID: 4f9af03138e6
Revises: 9a97b404cb53
Create Date: 2026-01-19 10:24:24.827541

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "4f9af03138e6"
down_revision = "9a97b404cb53"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем utm_term в marketplace_clients_list
    op.add_column(
        "marketplace_clients_list",
        sa.Column("utm_term", postgresql.ARRAY(sa.String()), nullable=True),
    )

    # Добавляем ref_user в marketplace_utm_tags
    op.add_column(
        "marketplace_utm_tags", sa.Column("ref_user", sa.String(), nullable=True)
    )


def downgrade() -> None:
    # Удаляем ref_user из marketplace_utm_tags
    op.drop_column("marketplace_utm_tags", "ref_user")

    # Удаляем utm_term из marketplace_clients_list
    op.drop_column("marketplace_clients_list", "utm_term")
