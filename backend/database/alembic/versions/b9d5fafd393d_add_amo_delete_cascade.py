"""add_amo_delete_cascade

Revision ID: b9d5fafd393d
Revises: 170143dc7ab4
Create Date: 2025-08-14 00:25:30.821723

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b9d5fafd393d"
down_revision = "170143dc7ab4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
            ALTER TABLE public.amo_entity_custom_fields
            DROP CONSTRAINT IF EXISTS amo_entity_custom_fields_contact_id_fkey;
            ALTER TABLE public.amo_entity_custom_fields
            ADD CONSTRAINT amo_entity_custom_fields_contact_id_fkey
            FOREIGN KEY (contact_id) REFERENCES public.amo_contacts(id) ON DELETE CASCADE;
    """
    )
    op.execute(
        """
            ALTER TABLE public.amo_table_contacts
            DROP CONSTRAINT IF EXISTS amo_table_contacts_amo_id_fkey;
            ALTER TABLE public.amo_table_contacts
            ADD CONSTRAINT amo_table_contacts_amo_id_fkey
            FOREIGN KEY (amo_id) REFERENCES public.amo_contacts(id) ON DELETE CASCADE;
    """
    )
    op.execute(
        """
            ALTER TABLE public.amo_contacts_double
            DROP CONSTRAINT IF EXISTS amo_contacts_double_orig_id_fkey;
            ALTER TABLE public.amo_contacts_double
            ADD CONSTRAINT amo_contacts_double_orig_id_fkey
            FOREIGN KEY (orig_id) REFERENCES public.amo_contacts(id) ON DELETE CASCADE;
    """
    )


def downgrade() -> None:
    op.execute(
        """
            ALTER TABLE public.amo_entity_custom_fields
            DROP CONSTRAINT IF EXISTS amo_entity_custom_fields_contact_id_fkey;
            ALTER TABLE public.amo_entity_custom_fields
            ADD CONSTRAINT amo_entity_custom_fields_contact_id_fkey
            FOREIGN KEY (contact_id) REFERENCES public.amo_contacts(id);
    """
    )
    op.execute(
        """
            ALTER TABLE public.amo_table_contacts
            DROP CONSTRAINT IF EXISTS amo_table_contacts_amo_id_fkey;
            ALTER TABLE public.amo_table_contacts
            ADD CONSTRAINT amo_table_contacts_amo_id_fkey
            FOREIGN KEY (amo_id) REFERENCES public.amo_contacts(id);
    """
    )
    op.execute(
        """
            ALTER TABLE public.amo_contacts_double
            DROP CONSTRAINT IF EXISTS amo_contacts_double_orig_id_fkey;
            ALTER TABLE public.amo_contacts_double
            ADD CONSTRAINT amo_contacts_double_orig_id_fkey
            FOREIGN KEY (orig_id) REFERENCES public.amo_contacts(id);
    """
    )
