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
def pytest_ydist_command_to_serializable(command: Command) -> dict | None:
    if command.__class__ in pytest_ydist_commands:
        command_data = asdict(command)
        command_data['status'] = command_data['status'].name
        command_data['kind'] = command.__class__.__name__
        return command_data


@pytest.hookimpl()
def pytest_ydist_command_from_serializable(command_data: dict) -> Command | None:
    kind = command_data.pop('kind')
    event_cls = next((e for e in pytest_ydist_commands if e.__name__ == kind), None)
    if event_cls is None:
        return
    command_data['status'] = CommandStatus[command_data['status']]
    return event_cls(**command_data)
