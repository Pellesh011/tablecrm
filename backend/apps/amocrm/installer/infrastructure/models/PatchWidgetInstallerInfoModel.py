from typing import Optional

from pydantic import BaseModel


class PatchWidgetInstallerInfoModel(BaseModel):
    client_name: Optional[str]
    client_cashbox: Optional[int]
    client_number_phone: Optional[str]

    partner_name: Optional[str]
    partner_cashbox: Optional[int]
    partner_number_phone: Optional[str]
    client_inn: Optional[str]
