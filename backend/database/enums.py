from enum import Enum


class Repeatability(str, Enum):
    minutes = "minutes"
    hours = "hours"
    days = "days"
    weeks = "weeks"
    months = "months"


class Gender(str, Enum):
    male = "Мужчина"
    female = "Женщина"


class ContragentType(str, Enum):
    company = "Компания"
    contact = "Контакт"


class TriggerType(str, Enum):
    before = "до"
    after = "после"


class TriggerTime(str, Enum):
    minute = "минуты"
    hour = "часы"
    day = "дни"


class DebitCreditType(str, Enum):
    debit = "debit"
    credit = "credit"
