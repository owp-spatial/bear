from enum import StrEnum
from typing import Self


class ProviderKind(StrEnum):
    openstreetmap = "openstreetmap"
    microsoft = "microsoft"
    usa_structures = "usa_structures"
    openaddresses = "openaddresses"
    nad = "nad"

    @classmethod
    def list_providers(cls) -> list[Self]:
        return list(map(lambda p: p, cls))
