from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Self

import pytest

from ydist import (
    commands as ydist_commands,
    types as ydist_types,
)

from ydist_resource import types


@dataclass
class RunTestsWithTokens(ydist_types.Command):
    run_test_command: ydist_commands.RunTests
    tokens: set[types.Token]

    @classmethod
    def from_serializable(cls, data: dict) -> Self:
        tokens = set()
        for token_data in data.pop('tokens'):
            kind = token_data.pop('kind')
            token_cls = types._token_types[kind]
            tokens.add(token_cls(**token_data))
        data['tokens'] = tokens
        data['run_test_command'] = ydist_commands.RunTests.from_serializable(data['run_test_command'])
        return super().from_serializable(data)

    def to_serializable(self) -> dict:
        data = super().to_serializable()
        data['tokens'] = [
            {'kind': token.__class__.__qualname__, **token.to_serializable()}
            for token in self.tokens
        ]
        data['run_test_command'] = self.run_test_command.to_serializable()
        return data


@pytest.hookimpl()
def pytest_ydist_register_commands() -> list[type[ydist_types.Command]]:
    return [RunTestsWithTokens]
