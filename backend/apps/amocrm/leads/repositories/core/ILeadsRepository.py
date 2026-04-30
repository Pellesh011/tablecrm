from apps.amocrm.leads.repositories.models.CreateLeadModel import CreateLeadModel


class ILeadsRepository:

    async def create_lead(
        self, access_token: str, amo_lead_model: CreateLeadModel, referrer: str
    ):
        raise NotImplementedError()
