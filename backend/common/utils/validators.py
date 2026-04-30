import math
from typing import Any, Optional


def sanitize_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return 0.0

    return float(value)
