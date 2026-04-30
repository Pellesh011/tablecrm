"""merge heads: apple wallet and feeds

Revision ID: merge_heads_001
Revises: ad90fb706d6a, 722d96cec7fb
Create Date: 2025-10-25 07:51:54.000000

"""

# revision identifiers, used by Alembic.
revision = "merge_heads_001"
down_revision = ("ad90fb706d6a", "722d96cec7fb")
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Merge migration - no changes needed
    pass


def downgrade() -> None:
    # Merge migration - no changes needed
    pass
