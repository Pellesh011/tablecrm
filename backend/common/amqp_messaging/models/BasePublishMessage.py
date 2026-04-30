from typing import Generic, TypeVar

from pydantic import BaseModel

from .BaseModelMessage import BaseModelMessage

E = TypeVar("E", bound=BaseModelMessage)


class BasePublishMessage(BaseModel, Generic[E]):
    event_name: str
    event: E
