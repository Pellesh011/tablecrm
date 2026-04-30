"""add tech_cards

Revision ID: ad90fb706d6a
Revises: 58249a41dca1
Create Date: 2025-10-23 20:58:35.938583

"""

# revision identifiers, used by Alembic.
revision = "ad90fb706d6a"
down_revision = "58249a41dca1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Tech cards already created in c9625a6c9c31
    # This is a no-op migration to maintain migration history
    pass


def downgrade() -> None:
    # No-op
    pass
