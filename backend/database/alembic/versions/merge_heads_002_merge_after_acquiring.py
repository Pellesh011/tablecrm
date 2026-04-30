"""merge heads after acquiring merge

Revision ID: merge_heads_002
Revises: ad1f4037a8b7
Create Date: 2025-11-27 21:55:14.868000

"""

# revision identifiers, used by Alembic.
revision = "merge_heads_002"
down_revision = (
    "ad1f4037a8b7"  # Исправлено: указываем на единственный head в ветке chat-system
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Merge migration - no changes needed
    pass


def downgrade() -> None:
    # Merge migration - no changes needed
    pass
