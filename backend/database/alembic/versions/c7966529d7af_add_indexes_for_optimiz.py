"""add indexes for optimiz

Revision ID: c7966529d7af
Revises: e8cbfae7d3e4
Create Date: 2026-04-16 02:15:32.376184

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "c7966529d7af"
down_revision = "e8cbfae7d3e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. tochka_bank_payments (19GB) — обратите внимание на экранирование "accountId"
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tbp_account_created
            ON tochka_bank_payments("accountId", created_at DESC);
        """
        )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tbp_payment_crm_id
            ON tochka_bank_payments(payment_crm_id) WHERE payment_crm_id IS NOT NULL;
        """
        )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tbp_status_created
            ON tochka_bank_payments(status, created_at DESC);
        """
        )

    # 2. contragents
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contragents_phone_cashbox
            ON contragents(phone, cashbox) WHERE is_deleted IS NOT TRUE;
        """
        )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contragents_cashbox_deleted
            ON contragents(cashbox, is_deleted, updated_at DESC);
        """
        )

    # 3. loyality_cards
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loyality_cards_contragent_cashbox
            ON loyality_cards(contragent_id, cashbox_id) WHERE is_deleted IS NOT TRUE;
        """
        )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loyality_cards_cashbox_deleted
            ON loyality_cards(cashbox_id, is_deleted, lifetime);
        """
        )

    # 4. amo_contacts
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_amo_contacts_phone
            ON amo_contacts(formatted_phone);
        """
        )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_amo_contacts_install_group
            ON amo_contacts(amo_install_group_id, updated_at DESC);
        """
        )

    # 5. amo_leads
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_amo_leads_contact_id
            ON amo_leads(contact_id) WHERE is_deleted IS NOT TRUE;
        """
        )

    # 6. payments
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payments_cashbox_date_deleted
            ON payments(cashbox, date DESC, is_deleted);
        """
        )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_payments_docs_sales_id
            ON payments(docs_sales_id) WHERE docs_sales_id IS NOT NULL;
        """
        )

    # 7. docs_sales
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_cashbox_updated
            ON docs_sales(cashbox, updated_at DESC) WHERE is_deleted IS NOT TRUE;
        """
        )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_docs_sales_contragent
            ON docs_sales(contragent, cashbox) WHERE is_deleted IS NOT TRUE;
        """
        )

    # 8. warehouse_register_movement
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wrm_org_warehouse_nomenclature
            ON warehouse_register_movement(organization_id, warehouse_id, nomenclature_id);
        """
        )

    # 9. events
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_cashbox_created
            ON events(cashbox_id, created_at DESC);
        """
        )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_token_created
            ON events(token, created_at DESC) WHERE token IS NOT NULL AND length(token) <= 255;
        """
        )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_tbp_account_created")
    op.execute("DROP INDEX IF EXISTS idx_tbp_payment_crm_id")
    op.execute("DROP INDEX IF EXISTS idx_tbp_status_created")
    op.execute("DROP INDEX IF EXISTS idx_contragents_phone_cashbox")
    op.execute("DROP INDEX IF EXISTS idx_contragents_cashbox_deleted")
    op.execute("DROP INDEX IF EXISTS idx_loyality_cards_contragent_cashbox")
    op.execute("DROP INDEX IF EXISTS idx_loyality_cards_cashbox_deleted")
    op.execute("DROP INDEX IF EXISTS idx_amo_contacts_phone")
    op.execute("DROP INDEX IF EXISTS idx_amo_contacts_install_group")
    op.execute("DROP INDEX IF EXISTS idx_amo_leads_contact_id")
    op.execute("DROP INDEX IF EXISTS idx_payments_cashbox_date_deleted")
    op.execute("DROP INDEX IF EXISTS idx_payments_docs_sales_id")
    op.execute("DROP INDEX IF EXISTS idx_docs_sales_cashbox_updated")
    op.execute("DROP INDEX IF EXISTS idx_docs_sales_contragent")
    op.execute("DROP INDEX IF EXISTS idx_wrm_org_warehouse_nomenclature")
    op.execute("DROP INDEX IF EXISTS idx_events_cashbox_created")
    op.execute("DROP INDEX IF EXISTS idx_events_token_created")
