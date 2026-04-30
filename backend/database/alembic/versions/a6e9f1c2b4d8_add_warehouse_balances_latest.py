"""add warehouse_balances_latest

Revision ID: a6e9f1c2b4d8
Revises: 911d5a30c5dc
Create Date: 2026-04-03 19:10:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a6e9f1c2b4d8"
down_revision = "911d5a30c5dc"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_matviews
                WHERE schemaname = 'public'
                  AND matviewname = 'warehouse_balances_latest'
            ) THEN
                EXECUTE 'DROP MATERIALIZED VIEW public.warehouse_balances_latest';
            END IF;
        END
        $$;
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_balances_latest (
            id              SERIAL PRIMARY KEY,
            organization_id INTEGER NOT NULL REFERENCES organizations(id),
            warehouse_id    INTEGER NOT NULL REFERENCES warehouses(id),
            nomenclature_id INTEGER NOT NULL REFERENCES nomenclature(id),
            cashbox_id      INTEGER REFERENCES cashboxes(id),
            current_amount  FLOAT NOT NULL DEFAULT 0,
            incoming_amount FLOAT NOT NULL DEFAULT 0,
            outgoing_amount FLOAT NOT NULL DEFAULT 0,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_wbl_org_wh_nom
                UNIQUE (organization_id, warehouse_id, nomenclature_id)
        )
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_wbl_wh_nom
        ON warehouse_balances_latest (warehouse_id, nomenclature_id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_wbl_org_wh
        ON warehouse_balances_latest (organization_id, warehouse_id)
        """
    )


def downgrade():
    op.execute("DROP TABLE IF EXISTS warehouse_balances_latest")
