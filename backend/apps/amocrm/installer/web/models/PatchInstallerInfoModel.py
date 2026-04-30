from typing import Optional

from pydantic import BaseModel


class PatchInstallerInfoModel(BaseModel):
    amo_account_id: int

    client_name: Optional[str]
    client_token: Optional[str]
    client_number_phone: Optional[str]

    partner_name: Optional[str]
    partner_token: Optional[str]
    partner_number_phone: Optional[str]
    client_inn: Optional[str]
