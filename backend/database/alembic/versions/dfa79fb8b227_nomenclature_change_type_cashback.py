"""nomenclature_change_type_cashback

Revision ID: dfa79fb8b227
Revises: 316365d6d617
Create Date: 2025-06-29 22:03:57.299778

"""

import sqlalchemy as sa
from alembic import op
from database.db import NomenclatureCashbackType

# revision identifiers, used by Alembic.
revision = "dfa79fb8b227"
down_revision = "316365d6d617"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "nomenclature",
        "cashback_type",
        existing_type=sa.Enum(NomenclatureCashbackType),
        server_default=sa.text("'lcard_cashback'"),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "nomenclature",
        "cashback_type",
        existing_type=sa.Enum(NomenclatureCashbackType),
        server_default=sa.text("'no_cashback'"),
        existing_nullable=False,
    )
