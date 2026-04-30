"""add_recipient_name_to_marketplace_orders

Revision ID: 3cdcb823124c
Revises: 2a9ef18fb082
Create Date: 2026-01-19 11:24:16.992423

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3cdcb823124c"
down_revision = "2a9ef18fb082"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем колонку recipient_name для хранения имени получателя из каждого заказа
    op.add_column(
        "marketplace_orders", sa.Column("recipient_name", sa.String(), nullable=True)
    )


def downgrade() -> None:
    # Удаляем колонку recipient_name
    op.drop_column("marketplace_orders", "recipient_name")
