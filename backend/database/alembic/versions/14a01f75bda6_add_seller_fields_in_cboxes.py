"""add seller fields in cboxes

Revision ID: 14a01f75bda6
Revises: 8b924d4c652d
Create Date: 2025-12-04 22:56:07.052595

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "14a01f75bda6"
down_revision = "8b924d4c652d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    try:
        op.drop_column("cashboxes", "description")
    except Exception:
        pass

    op.add_column("cashboxes", sa.Column("seller_name", sa.String()))
    op.add_column("cashboxes", sa.Column("seller_description", sa.Text()))
    op.add_column("cashboxes", sa.Column("seller_photo", sa.String()))


def downgrade():
    op.drop_column("cashboxes", "seller_name")
    op.drop_column("cashboxes", "seller_description")
    op.drop_column("cashboxes", "seller_photo")
