"""add nomenclatures to segment_object_type

Revision ID: 15f770dfcf11
Revises: merge_heads_c1f9_e1eef2d9
Create Date: 2026-03-02 00:48:58.732311

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "15f770dfcf11"
down_revision = "merge_heads_c1f9_e1eef2d9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    conn.execute(
        """
            ALTER TYPE segment_object_type
            ADD VALUE 'nomenclatures'
            AFTER 'contragents';
        """
    )


def downgrade() -> None:
    pass
