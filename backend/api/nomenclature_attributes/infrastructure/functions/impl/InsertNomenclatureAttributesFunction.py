from api.nomenclature_attributes.infrastructure.exceptions.NomenclatureAttributeNameAlreadyExistError import (
    NomenclatureAttributeNameAlreadyExistError,
)
from api.nomenclature_attributes.infrastructure.functions.core.IInsertNomenclatureAttributesFunction import (
    IInsertNomenclatureAttributesFunction,
)
from asyncpg import PostgresError, UniqueViolationError
from database.db import database, nomenclature_attributes


class InsertNomenclatureAttributesFunction(IInsertNomenclatureAttributesFunction):

    async def __call__(self, name: str, alias: str, cashbox_id: int) -> int:
        query = nomenclature_attributes.insert().values(
            name=name,
            alias=alias,
            cashbox=cashbox_id,
        )
        try:
            result = await database.fetch_one(query)
            return result.id
        except UniqueViolationError as error:
            self._parse_error(error, name)

    def _parse_error(self, err: PostgresError, name: str) -> None:
        if err.constraint_name == "uq_nomenclature_attributes_name_cashbox":  # type: ignore
            raise NomenclatureAttributeNameAlreadyExistError(name) from err
        else:
            raise err
