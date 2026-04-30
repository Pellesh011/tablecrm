"""tg bills bot payment date without time

Revision ID: 2f3c22da69da
Revises: d14f8e267dc1
Create Date: 2025-03-04 10:42:18.092334

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2f3c22da69da"
down_revision = "d14f8e267dc1"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "tg_bot_bills",
        "payment_date",
        existing_type=sa.DateTime(timezone=False),
        type_=sa.Date(),
        postgresql_using="payment_date::date",
    )


def downgrade():
    op.alter_column(
        "tg_bot_bills",
        "payment_date",
        existing_type=sa.Date(),
        type_=sa.DateTime(timezone=False),
        postgresql_using="payment_date::timestamp",
    )
