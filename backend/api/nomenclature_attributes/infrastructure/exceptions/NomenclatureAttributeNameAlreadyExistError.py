from dataclasses import dataclass


@dataclass(eq=False)
class NomenclatureAttributeNameAlreadyExistError(Exception):
    name: str

    @property
    def title(self) -> str:
        return f"A nomenclature attribute with the {self.name} name already exists"
