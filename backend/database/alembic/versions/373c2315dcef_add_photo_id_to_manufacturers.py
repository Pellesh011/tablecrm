"""add_photo_id_to_manufacturers

Revision ID: 373c2315dcef
Revises: 854e54846d57
Create Date: 2025-03-21 23:40:37.337556

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "373c2315dcef"
down_revision = "854e54846d57"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("manufacturers", sa.Column("photo_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_manufacturers_photo_id_pictures",
        "manufacturers",
        "pictures",
        ["photo_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_manufacturers_photo_id_pictures", "manufacturers", type_="foreignkey"
    )
    op.drop_column("manufacturers", "photo_id")
