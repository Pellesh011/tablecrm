"""add segements initial default items

Revision ID: 3283602b4401
Revises: d828f3b06324
Create Date: 2026-03-16 05:15:05.895499

"""

import json
import os

import sqlalchemy as sa
from alembic import op
from api.segments.common import generate_unix_salt_md5

# revision identifiers, used by Alembic.
revision = "3283602b4401"
down_revision = "d828f3b06324"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Load initial data from fixtures
    fixtures_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "initial_data", "segments.json"
    )

    if os.path.exists(fixtures_path):
        with open(fixtures_path, "r", encoding="UTF-8") as file:
            segments = json.load(file)

        if segments:
            for segment in segments:
                segment.setdefault("hash_tag", generate_unix_salt_md5())

            op.bulk_insert(
                sa.table(
                    "segments",
                    sa.column("name", sa.String()),
                    sa.column("description", sa.String()),
                    sa.column("type_of_update", sa.String()),
                    sa.column("update_settings", sa.JSON()),
                    sa.column("is_archived", sa.Boolean()),
                    sa.column("is_default", sa.Boolean()),
                    sa.column("criteria", sa.JSON()),
                    sa.column("actions", sa.JSON()),
                    sa.column("hash_tag", sa.String()),
                ),
                segments,
            )
            print(f"Loaded {len(segments)} segments from fixtures")
    else:
        print(f"Warning: fixtures file not found at {fixtures_path}")


def downgrade() -> None:
    pass
