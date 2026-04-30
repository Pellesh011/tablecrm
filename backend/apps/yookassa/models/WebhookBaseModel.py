from pydantic import BaseModel


class WebhookViewModel(BaseModel):
    event: str
    url: str


class WebhookBaseModel(WebhookViewModel):
    id: str
