from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Self

import pytest

from ydist import (
    events as ydist_events,
    types as ydist_types,
)

from ydist_resource import types

@dataclass
class TokensReleased(ydist_events.Event):
    tokens: set[types.Token]

    @classmethod
    def from_serializable(cls, data: dict) -> Self:
        tokens = set()
        for token_data in data.pop('tokens'):
            kind = token_data.pop('kind')
            token_cls = types._token_types[kind]
            tokens.add(token_cls(**token_data))
        data['tokens'] = tokens
        return super().from_serializable(data)

    def to_serializable(self) -> dict:
        data = super().to_serializable()
        data['tokens'] = [
            {'kind': token.__class__.__qualname__, **token.to_serializable()}
            for token in data['tokens']
        ]
        return data


@pytest.hookimpl()
def pytest_ydist_register_events() -> list[type[ydist_types.Event]]:
    return [TokensReleased]
