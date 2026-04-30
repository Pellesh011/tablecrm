"""add passiflora price type

Revision ID: c4e8f1d0a9b2
Revises: b7d1e3a9f2c4
Create Date: 2026-01-31 00:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "c4e8f1d0a9b2"
down_revision = "b7d1e3a9f2c4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO price_types (name, owner, cashbox, is_deleted, is_system)
        SELECT 'passiflora', relation_tg_cashboxes.id, relation_tg_cashboxes.cashbox_id, FALSE, FALSE
        FROM relation_tg_cashboxes
        WHERE NOT EXISTS(
          SELECT 1
          FROM price_types p
          WHERE p.owner = relation_tg_cashboxes.id AND p.name = 'passiflora' AND p.is_deleted IS NOT TRUE
        )
    """
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM price_types
        WHERE name = 'passiflora';
    """
    )
