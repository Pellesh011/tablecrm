from typing import Any, Dict, Set


def extract_doc_id(item: Dict[str, Any], ids: list[int]):
    doc_id = item.get("id")
    if doc_id is not None:
        ids.append(int(doc_id))


def extract_contragent_id(item: Dict[str, Any], ids: Set[int]):
    value = item.get("contragent")
    if value is not None:
        ids.add(value)


def extract_settings_id(item: Dict[str, Any], ids: list[int]):
    value = item.get("settings")
    if value is not None:
        ids.append(value)


def extract_user_ids(item: Dict[str, Any], ids: Set[int]):
    for field in ("assigned_picker", "assigned_courier"):
        value = item.get(field)
        if value is not None:
            ids.add(value)
