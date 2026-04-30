from typing import List

from pydantic import BaseModel


class CreateBookingRepeatLeadModel(BaseModel):
    id: int


class CreateBookingRepeatLeadAddModel(BaseModel):
    add: List[CreateBookingRepeatLeadModel]


class CreateBookingRepeatModel(BaseModel):
    leads: CreateBookingRepeatLeadAddModel
