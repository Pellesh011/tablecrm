"""add column client_id, rm column contragent_id

Revision ID: 271c36d38a0b
Revises: merge_heads_docs_sales_tilda
Create Date: 2026-02-12 21:11:38.310099

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "271c36d38a0b"
down_revision = "merge_heads_docs_sales_tilda"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # --- marketplace_reviews: add nullable, backfill, then NOT NULL ---
    op.add_column(
        "marketplace_reviews",
        sa.Column(
            "client_id",
            sa.BigInteger(),
            sa.ForeignKey("marketplace_clients_list.id"),
            nullable=True,
        ),
    )
    conn.execute(
        sa.text(
            """
            UPDATE marketplace_reviews r
            SET client_id = mcl.id
            FROM contragents c
            INNER JOIN marketplace_clients_list mcl ON mcl.phone = c.phone
            WHERE c.id = r.contagent_id
        """
        ),
    )
    conn.execute(
        sa.text(
            """
            UPDATE marketplace_reviews
            SET client_id = (SELECT id FROM marketplace_clients_list ORDER BY id LIMIT 1)
            WHERE client_id IS NULL
        """
        ),
    )
    op.alter_column(
        "marketplace_reviews",
        "client_id",
        existing_type=sa.BigInteger(),
        nullable=False,
    )
    op.drop_column("marketplace_reviews", "contagent_id")

    # --- marketplace_view_events: same ---
    op.add_column(
        "marketplace_view_events",
        sa.Column(
            "client_id",
            sa.BigInteger(),
            sa.ForeignKey("marketplace_clients_list.id"),
            nullable=True,
        ),
    )
    conn.execute(
        sa.text(
            """
            UPDATE marketplace_view_events v
            SET client_id = mcl.id
            FROM contragents c
            INNER JOIN marketplace_clients_list mcl ON mcl.phone = c.phone
            WHERE c.id = v.contragent_id
        """
        ),
    )
    conn.execute(
        sa.text(
            """
            UPDATE marketplace_view_events
            SET client_id = (SELECT id FROM marketplace_clients_list ORDER BY id LIMIT 1)
            WHERE client_id IS NULL
        """
        ),
    )
    op.alter_column(
        "marketplace_view_events",
        "client_id",
        existing_type=sa.BigInteger(),
        nullable=False,
    )
    op.drop_column("marketplace_view_events", "contragent_id")


def downgrade() -> None:
    # --- marketplace_view_events ---
    op.add_column(
        "marketplace_view_events",
        sa.Column(
            "contragent_id",
            sa.Integer(),
            sa.ForeignKey("contragents.id"),
            nullable=True,
        ),
    )
    op.drop_column("marketplace_view_events", "client_id")

    # --- marketplace_reviews ---
    op.add_column(
        "marketplace_reviews",
        sa.Column(
            "contagent_id",
            sa.Integer(),
            sa.ForeignKey("contragents.id"),
            nullable=False,
        ),
    )
    op.drop_column("marketplace_reviews", "client_id")
