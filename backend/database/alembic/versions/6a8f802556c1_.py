"""empty message

Revision ID: 6a8f802556c1
Revises: 5c9cbc4aa7c4
Create Date: 2025-08-16 17:12:16.199504

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6a8f802556c1"
down_revision = "5c9cbc4aa7c4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("docs_sales", sa.Column("priority", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("docs_sales", "priority")
