from __future__ import annotations

VAT_PERCENT_TO_CODE: dict[float, int] = {
    0.0: 1,  # без НДС
    10.0: 3,  # НДС 10%
    20.0: 4,  # НДС 20%
    5.0: 7,  # НДС 5%
    7.0: 8,  # НДС 7%
}


def vat_code_from_tax_percent(tax_percent: float | None) -> int:
    if tax_percent is None:
        return 1
    return VAT_PERCENT_TO_CODE.get(float(tax_percent), 1)
