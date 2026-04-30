from api.promocodes import models as promocodes_models
from api.tech_cards import models as tech_cards_models
from api.tech_operations import models as tech_operations_models
from database import db

__all__ = ["db", "tech_cards_models", "tech_operations_models", "promocodes_models"]
