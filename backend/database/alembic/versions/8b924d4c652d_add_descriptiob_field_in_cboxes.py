"""add descriptiob field in cboxes

Revision ID: 8b924d4c652d
Revises: ebd3d4fd346e
Create Date: 2025-12-03 15:21:14.665042

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8b924d4c652d"
down_revision = "ebd3d4fd346e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cashboxes", sa.Column("description", sa.Text()))


def downgrade():
    op.drop_column("cashboxes", "description")
