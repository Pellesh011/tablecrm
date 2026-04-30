"""change_svg_icon_to_text

Revision ID: change_svg_icon_to_text
Revises: fix_nomenclature_global_category_safe
Create Date: 2026-01-19 23:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "change_svg_icon_to_text"
down_revision = "fix_nomenclature_global_category_safe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Change svg_icon column type from String(255) to Text to support longer SVG strings."""
    op.alter_column(
        "channels",
        "svg_icon",
        existing_type=sa.String(length=255),
        type_=sa.Text(),
        existing_nullable=True,
    )


def downgrade() -> None:
    """Revert svg_icon column type back to String(255)."""
    op.alter_column(
        "channels",
        "svg_icon",
        existing_type=sa.Text(),
        type_=sa.String(length=255),
        existing_nullable=True,
    )
