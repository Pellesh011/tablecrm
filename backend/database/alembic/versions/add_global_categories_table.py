"""add global_categories table

Revision ID: add_global_categories
Revises: merge_heads_002, ace9991191d4
Create Date: 2025-12-01 00:00:00.000000

"""

import json
import os

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_global_categories"
down_revision = ("merge_heads_002", "ace9991191d4")
branch_labels = None
depends_on = None


def upgrade():
    """
    Creates global_categories table for marketplace category management.
    """
    op.create_table(
        "global_categories",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("code", sa.Integer(), nullable=True),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.Column("external_id", sa.String(), nullable=True),
        sa.Column("image_url", sa.String(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=True, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Index for parent_id for hierarchy queries
    op.create_index(
        "idx_global_categories_parent_id", "global_categories", ["parent_id"]
    )

    # Index for is_active for filtering active categories
    op.create_index(
        "idx_global_categories_is_active", "global_categories", ["is_active"]
    )

    # Index for external_id for external integrations
    op.create_index(
        "idx_global_categories_external_id", "global_categories", ["external_id"]
    )

    # Load initial data from fixtures
    fixtures_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "initial_data", "global_categories.json"
    )

    if os.path.exists(fixtures_path):
        with open(fixtures_path, "r", encoding="UTF-8") as file:
            categories = json.load(file)

        if categories:
            op.bulk_insert(
                sa.table(
                    "global_categories",
                    sa.column("id", sa.Integer()),
                    sa.column("name", sa.String()),
                    sa.column("parent_id", sa.Integer()),
                    sa.column("external_id", sa.String()),
                    sa.column("description", sa.String()),
                    sa.column("is_active", sa.Boolean()),
                ),
                categories,
            )
            print(f"Loaded {len(categories)} categories from fixtures")
    else:
        print(f"Warning: fixtures file not found at {fixtures_path}")


def downgrade():
    """
    Drops global_categories table.
    """
    op.drop_index("idx_global_categories_external_id", table_name="global_categories")
    op.drop_index("idx_global_categories_is_active", table_name="global_categories")
    op.drop_index("idx_global_categories_parent_id", table_name="global_categories")
    op.drop_table("global_categories")
