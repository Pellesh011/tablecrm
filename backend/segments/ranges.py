from datetime import datetime, timedelta, timezone


def apply_range(col, rng: dict, container: list):
    """
    Вспомогательная функция: добавляет выражения >=, <=, =, is_ в container.
    """
    if not rng:
        return
    if "gte" in rng:
        container.append(col >= rng["gte"])
    if "lte" in rng:
        container.append(col <= rng["lte"])
    if "eq" in rng:
        container.append(col == rng["eq"])

    if "is_" in rng:
        container.append(col.is_(rng["is_"]))
    if "is_none" in rng:
        container.append(col.is_(None) if rng["is_none"] else col.is_not(None))


def apply_date_range(col, rng: dict, container: list):
    now = datetime.now(timezone.utc)
    relative_keys = {"gte_seconds_ago", "lte_seconds_ago"}
    absolute_keys = {"gte", "lte"}

    has_relative = any(k in rng for k in relative_keys)
    has_absolute = any(k in rng for k in absolute_keys)

    new_rng = {}

    if has_relative:
        # Используем только относительные значения
        for key in relative_keys:
            if key in rng:
                base_key = key.replace("_seconds_ago", "")
                seconds = int(rng[key])
                new_rng[base_key] = now - timedelta(seconds=seconds)
    elif has_absolute:
        # Используем только абсолютные значения
        for k, v in rng.items():
            new_rng[k] = datetime.strptime(v, "%Y-%m-%d").date()

    if "is_none" in rng:
        new_rng["is_none"] = rng["is_none"]
    apply_range(col, new_rng, container)
