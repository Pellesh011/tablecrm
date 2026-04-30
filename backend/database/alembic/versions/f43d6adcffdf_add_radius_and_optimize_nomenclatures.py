"""add radius and optimize nomenclatures

Revision ID: f43d6adcffdf
Revises: 911d5a30c5dc
Create Date: 2026-04-04 07:01:48.313306

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f43d6adcffdf"
down_revision = "911d5a30c5dc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("prices", sa.Column("radius", sa.FLOAT, nullable=True))
    op.add_column(
        "prices",
        sa.Column(
            "hide_outside_radius", sa.BOOLEAN, server_default="false", nullable=False
        ),
    )

    op.add_column(
        "tech_operations", sa.Column("sale_write_off_doc_id", sa.INTEGER, nullable=True)
    )
    op.create_foreign_key(
        "fk_tech_operations_sale_write_off_doc_id",
        "tech_operations",
        "docs_warehouse",
        ["sale_write_off_doc_id"],
        ["id"],
    )

    op.execute("COMMIT")

    try:
        # Частичный индекс на radius (только не NULL)
        op.create_index(
            "idx_prices_radius",
            "prices",
            ["radius"],
            unique=False,
            postgresql_concurrently=True,
            postgresql_where=sa.text("radius IS NOT NULL"),
        )

        # Частичный индекс на hide_outside_radius (только TRUE)
        op.create_index(
            "idx_prices_hide_radius",
            "prices",
            ["hide_outside_radius"],
            unique=False,
            postgresql_concurrently=True,
            postgresql_where=sa.text("hide_outside_radius = TRUE"),
        )

        # Обычный индекс на nomenclature_videos.nomenclature_id
        op.create_index(
            "idx_nomenclature_videos_nom_id",
            "nomenclature_videos",
            ["nomenclature_id"],
            unique=False,
            postgresql_concurrently=True,
        )

        # Обычный индекс на nomenclature_attributes_value.nomenclature_id
        op.create_index(
            "idx_nomenclature_attributes_value_nom",
            "nomenclature_attributes_value",
            ["nomenclature_id"],
            unique=False,
            postgresql_concurrently=True,
        )
    finally:
        op.execute("BEGIN")


def downgrade() -> None:
    op.drop_index("idx_prices_radius", table_name="prices")
    op.drop_index("idx_prices_hide_radius", table_name="prices")
    op.drop_index("idx_nomenclature_videos_nom_id", table_name="nomenclature_videos")
    op.drop_index(
        "idx_nomenclature_attributes_value_nom",
        table_name="nomenclature_attributes_value",
    )

    op.drop_constraint(
        "fk_tech_operations_sale_write_off_doc_id",
        "tech_operations",
        type_="foreignkey",
    )
    op.drop_column("tech_operations", "sale_write_off_doc_id")

    op.drop_column("prices", "radius")
    op.drop_column("prices", "hide_outside_radius")
