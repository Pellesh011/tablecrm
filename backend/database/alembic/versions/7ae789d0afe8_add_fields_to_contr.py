"""add fields to contr

Revision ID: 7ae789d0afe8
Revises: fab805a9d974
Create Date: 2024-11-09 23:23:29.060643

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "7ae789d0afe8"
down_revision = "fab805a9d974"
branch_labels = None
depends_on = None


def upgrade() -> None:
    gender_enum = postgresql.ENUM("Мужчина", "Женщина", name="gender")
    gender_enum.create(op.get_bind())
    op.add_column(
        "contragents", sa.Column("additional_phones", sa.String(), nullable=True)
    )
    op.add_column(
        "contragents",
        sa.Column("gender", sa.Enum("male", "female", name="gender"), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("contragents", "gender")
    op.drop_column("contragents", "additional_phones")
    gender_enum = postgresql.ENUM("Мужчина", "Женщина", name="gender")
    gender_enum.drop(op.get_bind())
