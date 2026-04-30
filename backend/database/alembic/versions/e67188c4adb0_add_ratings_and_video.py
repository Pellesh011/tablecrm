"""add ratings and video

Revision ID: e67188c4adb0
Revises: 86b5ace60337
Create Date: 2026-02-16 17:43:13.394250

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e67188c4adb0"
down_revision = "86b5ace60337"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "nomenclature",
        sa.Column(
            "rating_details",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text(
                '\n\'{\n  "photo": 10,\n  "video": 50,\n  "seo": 10,\n  "description": 10,\n  "category": 10\n}\'::jsonb\n'
            ),
            nullable=False,
        ),
    )
    op.add_column("nomenclature", sa.Column("rating", sa.Integer(), nullable=True))
    op.add_column("nomenclature", sa.Column("video_link", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("nomenclature", "video_link")
    op.drop_column("nomenclature", "rating")
    op.drop_column("nomenclature", "rating_details")
