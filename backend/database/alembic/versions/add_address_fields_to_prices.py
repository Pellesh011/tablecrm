"""add address fields to prices

Revision ID: add_address_to_prices
Revises: optimize_segments_003
Create Date: 2025-01-20 13:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_address_to_prices"
down_revision = "optimize_segments_003"
branch_labels = None
depends_on = None


def upgrade():
    """Add address, latitude, and longitude fields to prices table"""
    op.add_column("prices", sa.Column("address", sa.String(), nullable=True))
    op.add_column("prices", sa.Column("latitude", sa.Float(), nullable=True))
    op.add_column("prices", sa.Column("longitude", sa.Float(), nullable=True))


def downgrade():
    """Remove address fields from prices table"""
    op.drop_column("prices", "longitude")
    op.drop_column("prices", "latitude")
    op.drop_column("prices", "address")
