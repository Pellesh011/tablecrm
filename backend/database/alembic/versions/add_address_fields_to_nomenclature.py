"""add address fields to nomenclature

Revision ID: add_address_to_nomenclature
Revises: add_address_to_prices
Create Date: 2025-01-20 13:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_address_to_nomenclature"
down_revision = "add_address_to_prices"
branch_labels = None
depends_on = None


def upgrade():
    """Add address, latitude, and longitude fields to nomenclature table"""
    op.add_column("nomenclature", sa.Column("address", sa.String(), nullable=True))
    op.add_column("nomenclature", sa.Column("latitude", sa.Float(), nullable=True))
    op.add_column("nomenclature", sa.Column("longitude", sa.Float(), nullable=True))


def downgrade():
    """Remove address fields from nomenclature table"""
    op.drop_column("nomenclature", "longitude")
    op.drop_column("nomenclature", "latitude")
    op.drop_column("nomenclature", "address")
