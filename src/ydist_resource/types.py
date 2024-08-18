from __future__ import annotations

import pytest

from dataclasses import dataclass, asdict
from typing import Hashable


@dataclass
class Token:
    token: Hashable

    def __hash__(self):
        return hash((self.__class__.__qualname__, self.token))


@dataclass
class CollectionId:
    collection_id: Hashable

    def __hash__(self):
        return hash((self.__class__.__qualname__, self.collection_id))


@pytest.hookimpl()
def pytest_ydist_resource_token_to_serializable(
    config: pytest.Config,
    token: Token,
) -> dict | None:
    if token.__class__ is Token:
        token_data = asdict(token)
        token_data['kind'] = Token.__qualname__
        return token_data


@pytest.hookimpl()
def pytest_ydist_resource_token_from_serializable(
    config: pytest.Config,
    token_data: dict,
) -> Token | None:
    if token_data['kind'] == Token.__qualname__:
        token_data.pop('kind')
        return Token(**token_data)
