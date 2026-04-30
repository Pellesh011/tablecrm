"""Generate QR hashes for all existing nomenclature items and warehouses

Revision ID: generate_qr_hashes_for_all_nomenclature
Revises: merge_address_apple_wallet
Create Date: 2025-01-20 14:00:00.000000

"""

import hashlib
from datetime import datetime

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "generate_qr_hashes_for_all_nomenclature"
down_revision = "merge_address_apple_wallet"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Generate QR hashes for all existing nomenclature items and warehouses that don't have one.
    Nomenclature hash format: nm_ + sha256(id:name:article)[:16]
    Warehouse hash format: wh_ + sha256(WH:id:name)[:16]
    """
    connection = op.get_bind()

    # ========== NOMENCLATURE HASHES ==========
    # Find all nomenclature items without hash
    query = text(
        """
        SELECT n.id, n.name
        FROM nomenclature n
        LEFT JOIN nomenclature_hash nh ON nh.nomenclature_id = n.id
        WHERE nh.nomenclature_id IS NULL
        AND n.is_deleted IS NOT TRUE
    """
    )

    result = connection.execute(query)
    items_to_hash = result.fetchall()

    if items_to_hash:
        # Generate hashes and insert them
        hash_values = []
        for item in items_to_hash:
            nom_id = item[0]
            nom_name = item[1] or ""
            # article field doesn't exist in DB, use empty string as in code
            nom_article = ""

            hash_base = f"{nom_id}:{nom_name}:{nom_article}"
            hash_string = "nm_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]

            hash_values.append(
                {
                    "nomenclature_id": nom_id,
                    "hash": hash_string,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
            )

        # Insert hashes in batches
        if hash_values:
            insert_query = text(
                """
                INSERT INTO nomenclature_hash (nomenclature_id, hash, created_at, updated_at)
                VALUES (:nomenclature_id, :hash, :created_at, :updated_at)
                ON CONFLICT DO NOTHING
            """
            )

            # Insert in batches of 100
            batch_size = 100
            for i in range(0, len(hash_values), batch_size):
                batch = hash_values[i : i + batch_size]
                for item in batch:
                    connection.execute(
                        insert_query,
                        {
                            "nomenclature_id": item["nomenclature_id"],
                            "hash": item["hash"],
                            "created_at": item["created_at"],
                            "updated_at": item["updated_at"],
                        },
                    )

    # ========== WAREHOUSE HASHES ==========
    # Find all warehouses (including those with old format hashes)
    warehouse_query = text(
        """
        SELECT w.id, w.name, wh.hash as existing_hash
        FROM warehouses w
        LEFT JOIN warehouse_hash wh ON wh.warehouses_id = w.id
        WHERE w.is_deleted IS NOT TRUE
    """
    )

    warehouse_result = connection.execute(warehouse_query)
    warehouses_to_process = warehouse_result.fetchall()

    if warehouses_to_process:
        # Generate hashes for all warehouses (create new or update old format)
        warehouse_hash_values = []
        for warehouse in warehouses_to_process:
            wh_id = warehouse[0]
            wh_name = warehouse[1] or ""
            existing_hash = warehouse[2]

            # Generate new format hash: wh_ + SHA256(WH:id:name)[:16]
            hash_base = f"WH:{wh_id}:{wh_name}"
            hash_string = "wh_" + hashlib.sha256(hash_base.encode()).hexdigest()[:16]

            # Update if hash doesn't exist or is in old format (starts with "warehouses_")
            if not existing_hash or existing_hash.startswith("warehouses_"):
                warehouse_hash_values.append(
                    {
                        "warehouses_id": wh_id,
                        "hash": hash_string,
                        "created_at": datetime.now(),
                        "updated_at": datetime.now(),
                    }
                )

        # Insert/update hashes in batches
        if warehouse_hash_values:
            # Separate UPDATE and INSERT queries for reliability
            warehouse_update_query = text(
                """
                UPDATE warehouse_hash
                SET hash = :hash, updated_at = :updated_at
                WHERE warehouses_id = :warehouses_id
            """
            )

            warehouse_insert_query = text(
                """
                INSERT INTO warehouse_hash (warehouses_id, hash, created_at, updated_at)
                VALUES (:warehouses_id, :hash, :created_at, :updated_at)
            """
            )

            # Process in batches
            batch_size = 100
            for i in range(0, len(warehouse_hash_values), batch_size):
                batch = warehouse_hash_values[i : i + batch_size]
                for item in batch:
                    # Try to update existing record first
                    update_result = connection.execute(
                        warehouse_update_query,
                        {
                            "warehouses_id": item["warehouses_id"],
                            "hash": item["hash"],
                            "updated_at": item["updated_at"],
                        },
                    )

                    # If no rows were updated, insert new record
                    if update_result.rowcount == 0:
                        connection.execute(
                            warehouse_insert_query,
                            {
                                "warehouses_id": item["warehouses_id"],
                                "hash": item["hash"],
                                "created_at": item["created_at"],
                                "updated_at": item["updated_at"],
                            },
                        )
                # Alembic handles transactions automatically, no need for commit()


def downgrade() -> None:
    """
    Note: This migration only adds missing hashes for nomenclature and warehouses.
    We don't remove hashes that were added by this migration,
    as we can't distinguish them from manually created ones.
    """
    pass
