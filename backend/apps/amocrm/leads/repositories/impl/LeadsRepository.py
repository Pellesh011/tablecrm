import aiohttp
from apps.amocrm.leads.repositories.core.ILeadsRepository import ILeadsRepository
from apps.amocrm.leads.repositories.models.CreateLeadModel import CreateLeadModel


class LeadsRepository(ILeadsRepository):

    def __init__(self):
        self.__base_url = "https://{}/api/v4/leads/complex"

    async def create_lead(
        self, access_token: str, amo_lead_model: CreateLeadModel, referrer: str
    ):
        async with aiohttp.ClientSession(trust_env=True) as http_session:
            async with http_session.post(
                self.__base_url.format(referrer),
                json=[
                    amo_lead_model.dict(
                        exclude_defaults=True, exclude_none=True, by_alias=True
                    )
                ],
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-type": "application/json",
                },
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    print(resp.status)
                    print(await resp.text())
                    resp.raise_for_status()
