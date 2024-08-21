from __future__ import annotations

import pytest

from dataclasses import dataclass, asdict
from typing import Hashable

from ydist.types import Serializable

@dataclass
class Token(Serializable):
    token: Hashable

    def __hash__(self):
        return hash((self.__class__.__qualname__, self.token))


@dataclass
class CollectionId:
    collection_id: Hashable

    def __hash__(self):
        return hash((self.__class__.__qualname__, self.collection_id))


# This is a pretty ugly hack to allow the Token deserialization to work
# This will get populated by the plugin with all the registered token types, hopefully before
#  any token serialization/deserialization takes place
_token_types = {}

@pytest.hookspec
def pytest_ydist_resource_register_tokens() -> list[type[Token]]:
    return [Token]
