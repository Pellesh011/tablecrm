"""merge address and apple wallet migrations

Revision ID: merge_address_apple_wallet
Revises: add_address_to_nomenclature, 3cdcb823124c
Create Date: 2025-01-20 15:00:00.000000

"""

# revision identifiers, used by Alembic.
revision = "merge_address_apple_wallet"
down_revision = ("add_address_to_nomenclature", "3cdcb823124c")
branch_labels = None
depends_on = None


def upgrade():
    """Merge migration - no changes needed"""
    pass


def downgrade():
    """Merge migration - no changes needed"""
    pass
