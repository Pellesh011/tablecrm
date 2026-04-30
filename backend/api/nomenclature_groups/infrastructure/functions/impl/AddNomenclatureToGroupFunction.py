from api.nomenclature_groups.infrastructure.exceptions.GroupAlreadyHaveIsMainError import (
    GroupAlreadyHaveIsMainError,
)
from api.nomenclature_groups.infrastructure.exceptions.NomenclatureIdAlreadyExistsInGroupError import (
    NomenclatureIdAlreadyExistsInGroupError,
)
from api.nomenclature_groups.infrastructure.functions.core.IAddNomenclatureToGroupFunction import (
    IAddNomenclatureToGroupFunction,
)
from asyncpg import PostgresError, UniqueViolationError
from database.db import database, nomenclature_groups_value


class AddNomenclatureToGroupFunction(IAddNomenclatureToGroupFunction):

    async def __call__(self, group_id: int, nomenclature_id: int, is_main: bool) -> int:
        query = (
            nomenclature_groups_value.insert()
            .values(
                nomenclature_id=nomenclature_id,
                group_id=group_id,
                is_main=is_main,
            )
            .returning(nomenclature_groups_value.c.id)
        )
        try:
            result = await database.fetch_one(query)
            return result.id
        except UniqueViolationError as error:
            self._parse_error(error, group_id, nomenclature_id)

    def _parse_error(
        self, err: PostgresError, group_id: int, nomenclature_id: int
    ) -> None:
        if err.constraint_name == "uq_nomenclature_groups_value_nomenclature_id":  # type: ignore
            raise NomenclatureIdAlreadyExistsInGroupError(
                nomenclature_id, group_id
            ) from err
        elif err.constraint_name == "uq_nomenclature_groups_value_group_id_is_main":  # type: ignore
            raise GroupAlreadyHaveIsMainError(group_id) from err
        else:
            raise err
