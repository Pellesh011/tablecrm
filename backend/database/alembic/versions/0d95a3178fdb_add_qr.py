"""add qr

Revision ID: 0d95a3178fdb
Revises: fix_chat_contact_id
Create Date: 2026-04-11 14:45:41.293848

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0d95a3178fdb"
down_revision = "fix_chat_contact_id"
branch_labels = None
depends_on = None


def upgrade():
    # ── qr_pages ────────────────────────────────────────────────────────────
    op.create_table(
        "qr_pages",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "cashbox_id", sa.Integer(), sa.ForeignKey("cashboxes.id"), nullable=False
        ),
        sa.Column("source_name", sa.String(255), nullable=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("image_url", sa.Text(), nullable=True),
        sa.Column("logo", sa.Text(), nullable=True),
        sa.Column("buttons", sa.JSON(), nullable=True),
        sa.Column("short_code", sa.String(50), nullable=False, unique=True),
        sa.Column("utm_source", sa.String(100), nullable=True),
        sa.Column("utm_medium", sa.String(100), nullable=True),
        sa.Column("utm_campaign", sa.String(100), nullable=True),
        sa.Column(
            "collect_form_utm", sa.Boolean(), nullable=False, server_default=sa.true()
        ),
        sa.Column("auto_tags", postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
        sa.Column(
            "is_deleted", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )
    op.create_index("ix_qr_pages_cashbox_id", "qr_pages", ["cashbox_id"])
    op.create_index("ix_qr_pages_short_code", "qr_pages", ["short_code"], unique=True)

    # ── qr_visits ───────────────────────────────────────────────────────────
    op.create_table(
        "qr_visits",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "page_id", sa.Integer(), sa.ForeignKey("qr_pages.id"), nullable=False
        ),
        sa.Column("button_code", sa.String(50), nullable=True),
        sa.Column("ip", sa.String(60), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("referer", sa.String(500), nullable=True),
        sa.Column("utm_params", sa.JSON(), nullable=True),
        sa.Column("yandex_cid", sa.String(100), nullable=True),
        sa.Column("google_cid", sa.String(100), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )
    op.create_index("ix_qr_visits_page_id", "qr_visits", ["page_id"])

    # ── qr_targets ──────────────────────────────────────────────────────────
    op.create_table(
        "qr_targets",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "visit_id", sa.Integer(), sa.ForeignKey("qr_visits.id"), nullable=False
        ),
        sa.Column(
            "page_id", sa.Integer(), sa.ForeignKey("qr_pages.id"), nullable=False
        ),
        sa.Column("button_type", sa.String(50), nullable=False),
        sa.Column(
            "registered", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
    )
    op.create_index("ix_qr_targets_page_id", "qr_targets", ["page_id"])
    op.create_index("ix_qr_targets_visit_id", "qr_targets", ["visit_id"])


def downgrade():
    op.drop_table("qr_targets")
    op.drop_table("qr_visits")
    op.drop_table("qr_pages")
