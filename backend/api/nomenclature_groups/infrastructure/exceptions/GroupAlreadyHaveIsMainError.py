from dataclasses import dataclass


@dataclass(eq=False)
class GroupAlreadyHaveIsMainError(Exception):
    group_id: int

    @property
    def title(self) -> str:
        return (
            f"A group with the {self.group_id} id already exists is main nomenclature"
        )
