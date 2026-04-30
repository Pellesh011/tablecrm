"""add tech_card parent nomenclature and nullable items fields

Revision ID: 4a8f2f6c0c21
Revises: 76d4216e5d87
Create Date: 2026-02-05 15:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4a8f2f6c0c21"
down_revision: Union[str, None] = "merge_heads_docs_sales_tilda"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tech_cards",
        sa.Column("parent_nomenclature_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_tech_cards_parent_nomenclature",
        "tech_cards",
        "nomenclature",
        ["parent_nomenclature_id"],
        ["id"],
    )

    op.alter_column("tech_card_items", "type_of_processing", nullable=True)
    op.alter_column("tech_card_items", "waste_from_cold_processing", nullable=True)
    op.alter_column("tech_card_items", "waste_from_heat_processing", nullable=True)
    op.alter_column("tech_card_items", "net_weight", nullable=True)
    op.alter_column("tech_card_items", "gross_weight", nullable=True)
    op.alter_column("tech_card_items", "output", nullable=True)


def downgrade() -> None:
    op.alter_column("tech_card_items", "output", nullable=False)
    op.alter_column("tech_card_items", "gross_weight", nullable=False)
    op.alter_column("tech_card_items", "net_weight", nullable=False)
    op.alter_column("tech_card_items", "waste_from_heat_processing", nullable=False)
    op.alter_column("tech_card_items", "waste_from_cold_processing", nullable=False)
    op.alter_column("tech_card_items", "type_of_processing", nullable=False)

    op.drop_constraint(
        "fk_tech_cards_parent_nomenclature", "tech_cards", type_="foreignkey"
    )
    op.drop_column("tech_cards", "parent_nomenclature_id")
