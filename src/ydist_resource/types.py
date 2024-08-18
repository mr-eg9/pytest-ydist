from __future__ import annotations

from dataclasses import dataclass
from typing import Hashable

@dataclass
class Token:
    token: Hashable

    def __hash__(self):
        return hash((self.__class__, self.token))


@dataclass
class CollectionId:
    collection_id: Hashable

    def __hash__(self):
        return hash((self.__class__, self.collection_id))
