"""Change type data

Revision ID: 4979093a5519
Revises: 2f3c22da69da
Create Date: 2025-03-10 18:43:13.081531

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4979093a5519"
down_revision = "2f3c22da69da"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "loyality_cards",  # Имя таблицы
        "lifetime",  # Имя столбца
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        postgresql_using="lifetime::bigint",
    )


def downgrade() -> None:
    pass
