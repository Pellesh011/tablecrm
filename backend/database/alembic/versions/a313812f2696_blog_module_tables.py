"""blog module tables

Revision ID: a313812f2696
Revises: apscheduler_jobs_001
Create Date: 2026-02-11 20:41:19.895810

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a313812f2696"
down_revision = "apscheduler_jobs_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # blog_sites
    op.create_table(
        "blog_sites",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "cashbox_id",
            sa.Integer(),
            sa.ForeignKey("cashboxes.id"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("token", sa.String(length=255), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("token", name="uq_blog_sites_token"),
    )
    op.create_index("ix_blog_sites_cashbox_id", "blog_sites", ["cashbox_id"])

    # blog_folders
    op.create_table(
        "blog_folders",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "cashbox_id",
            sa.Integer(),
            sa.ForeignKey("cashboxes.id"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column(
            "parent_id",
            sa.BigInteger(),
            sa.ForeignKey("blog_folders.id"),
            nullable=True,
        ),
        sa.Column("path", sa.String(length=1024), nullable=False),
        sa.Column("settings", sa.JSON(), nullable=True),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "cashbox_id",
            "parent_id",
            "slug",
            name="uq_blog_folders_cashbox_parent_slug",
        ),
    )
    op.create_index(
        "ix_blog_folders_cashbox_parent",
        "blog_folders",
        ["cashbox_id", "parent_id"],
    )
    op.create_index(
        "ix_blog_folders_cashbox_path",
        "blog_folders",
        ["cashbox_id", "path"],
    )

    # blog_site_folders
    op.create_table(
        "blog_site_folders",
        sa.Column(
            "site_id",
            sa.BigInteger(),
            sa.ForeignKey("blog_sites.id"),
            primary_key=True,
        ),
        sa.Column(
            "folder_id",
            sa.BigInteger(),
            sa.ForeignKey("blog_folders.id"),
            primary_key=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_blog_site_folders_folder",
        "blog_site_folders",
        ["folder_id"],
    )

    # blog_posts
    op.create_table(
        "blog_posts",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "cashbox_id",
            sa.Integer(),
            sa.ForeignKey("cashboxes.id"),
            nullable=False,
        ),
        sa.Column(
            "folder_id",
            sa.BigInteger(),
            sa.ForeignKey("blog_folders.id"),
            nullable=False,
        ),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("excerpt", sa.Text(), nullable=True),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default="DRAFT",
        ),
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("meta", sa.JSON(), nullable=True),
        sa.Column(
            "is_deleted",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("cashbox_id", "slug", name="uq_blog_posts_cashbox_slug"),
    )
    op.create_index(
        "ix_blog_posts_status_published_at",
        "blog_posts",
        ["status", "published_at"],
    )

    # blog_tags
    op.create_table(
        "blog_tags",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "cashbox_id",
            sa.Integer(),
            sa.ForeignKey("cashboxes.id"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("cashbox_id", "slug", name="uq_blog_tags_cashbox_slug"),
    )

    # blog_post_tags
    op.create_table(
        "blog_post_tags",
        sa.Column(
            "post_id",
            sa.BigInteger(),
            sa.ForeignKey("blog_posts.id"),
            primary_key=True,
        ),
        sa.Column(
            "tag_id",
            sa.BigInteger(),
            sa.ForeignKey("blog_tags.id"),
            primary_key=True,
        ),
    )
    op.create_index(
        "ix_blog_post_tags_tag",
        "blog_post_tags",
        ["tag_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_blog_post_tags_tag", table_name="blog_post_tags")
    op.drop_table("blog_post_tags")

    op.drop_table("blog_tags")

    op.drop_index("ix_blog_posts_status_published_at", table_name="blog_posts")
    op.drop_table("blog_posts")

    op.drop_index("ix_blog_site_folders_folder", table_name="blog_site_folders")
    op.drop_table("blog_site_folders")

    op.drop_index("ix_blog_folders_cashbox_path", table_name="blog_folders")
    op.drop_index("ix_blog_folders_cashbox_parent", table_name="blog_folders")
    op.drop_table("blog_folders")

    op.drop_index("ix_blog_sites_cashbox_id", table_name="blog_sites")
    op.drop_table("blog_sites")
