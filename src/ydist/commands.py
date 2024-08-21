from __future__ import annotations

from dataclasses import dataclass, asdict
import pytest

from ydist.types import Command, TestIdx, CommandStatus

@dataclass
class RunTests(Command):
    tests: list[TestIdx]


@dataclass
class ShutdownWorker(Command):
    pass


# All commands that are part of ydist
pytest_ydist_commands = {
    RunTests,
    ShutdownWorker,
}

@pytest.hookimpl()
def pytest_ydist_register_commands() -> list[type[Command]]:
    """Register command types for use with ydist.

    This is used by ydist to be able to deserialize commands.
    """
    return list(pytest_ydist_commands)
