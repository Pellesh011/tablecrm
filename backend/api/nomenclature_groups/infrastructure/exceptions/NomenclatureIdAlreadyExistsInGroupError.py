from dataclasses import dataclass


@dataclass(eq=False)
class NomenclatureIdAlreadyExistsInGroupError(Exception):
    nomenclature_id: int
    group_id: int

    @property
    def title(self) -> str:
        return f"A nomenclature with the {self.nomenclature_id} id already exists in group {self.group_id} id"
