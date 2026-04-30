"""add contr type

Revision ID: 8eb1872d5402
Revises: 7ae789d0afe8
Create Date: 2024-11-25 16:10:44.112216

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "8eb1872d5402"
down_revision = "7ae789d0afe8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    contrtype_enum = postgresql.ENUM("Компания", "Контакт", name="contrtype")
    contrtype_enum.create(op.get_bind())
    op.add_column(
        "contragents",
        sa.Column(
            "type", sa.Enum("company", "contact", name="contrtype"), nullable=True
        ),
    )


def downgrade() -> None:
    op.drop_column("contragents", "type")
    contrtype_enum = postgresql.ENUM("Компания", "Контакт", name="contrtype")
    contrtype_enum.drop(op.get_bind())
