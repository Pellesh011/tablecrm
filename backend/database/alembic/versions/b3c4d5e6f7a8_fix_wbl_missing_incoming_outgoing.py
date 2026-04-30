"""fix missing incoming/outgoing in warehouse_balances_latest

Revision ID: b3c4d5e6f7a8
Revises: a6e9f1c2b4d8
Create Date: 2026-04-04 13:15:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b3c4d5e6f7a8"
down_revision = "a6e9f1c2b4d8"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        DO $$
        DECLARE
            rel_kind "char";
        BEGIN
            SELECT c.relkind
            INTO rel_kind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = 'warehouse_balances_latest'
            LIMIT 1;

            IF rel_kind = 'm' THEN
                RAISE EXCEPTION
                    'warehouse_balances_latest is still a materialized view. Apply migration a6e9f1c2b4d8 first.';
            END IF;

            IF rel_kind = 'r' THEN
                ALTER TABLE public.warehouse_balances_latest
                    ADD COLUMN IF NOT EXISTS incoming_amount FLOAT NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS outgoing_amount FLOAT NOT NULL DEFAULT 0;
            END IF;
        END
        $$;
        """
    )


def downgrade():
    op.execute(
        """
        DO $$
        DECLARE
            rel_kind "char";
        BEGIN
            SELECT c.relkind
            INTO rel_kind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = 'warehouse_balances_latest'
            LIMIT 1;

            IF rel_kind = 'r' THEN
                ALTER TABLE public.warehouse_balances_latest
                    DROP COLUMN IF EXISTS incoming_amount,
                    DROP COLUMN IF EXISTS outgoing_amount;
            END IF;
        END
        $$;
        """
    )
