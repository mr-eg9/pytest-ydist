from __future__ import annotations

import pytest
from dataclasses import dataclass, asdict
from typing import Any, Self

from ydist.types import SeqNr, CommandStatus, Event, TestIdx


@dataclass
class CommandChangedStatus(Event):
    seq_nr: SeqNr
    new_status: CommandStatus

    @classmethod
    def from_serializable(cls, data: dict) -> Self:
        data['new_status'] = CommandStatus[data['new_status']]
        return cls(**data)

    def to_serializable(self) -> dict:
        data = asdict(self)
        data['new_status'] = data['new_status'].name
        return data


@dataclass
class WorkerStarted(Event):
    pass


@dataclass
class WorkerShutdown(Event):
    pass


@dataclass
class TestComplete(Event):
    test_idx: TestIdx

@dataclass
class SessionStart(Event):
    pass

@dataclass
class SessionFinish(Event):
    exitstatus: int

@dataclass
class Collection(Event):
    pass

@dataclass
class CollectionFinish(Event):
    pass

@dataclass
class RuntestLogstart(Event):
    nodeid: str
    location: tuple[str, int | None, str]

@dataclass
class RuntestLogfinish(Event):
    nodeid: str
    location: tuple[str, int | None, str]

@dataclass
class RuntestLogreport(Event):
    report: Any

@dataclass
class RuntestCollectreport(Event):
    report: Any

@dataclass
class RuntestWarningRecorded(Event):
    warning_message: SerializableWarningMessage
    when: str
    nodeid: str
    location: tuple[str, int, str] | None

    @classmethod
    def from_serializable(cls, data: dict) -> Self:
        data['warning_message'] = SerializableWarningMessage(**data['warning_message'])
        return cls(**data)

@dataclass
class SerializableWarningMessage:
    message: str
    cls_module: str
    cls_name: str
    filename: str
    lineno: int
    source: str


# All events that are part of ydist
pytest_ydist_events: set[type[Event]] = {
    CommandChangedStatus,
    WorkerStarted,
    WorkerShutdown,
    TestComplete,
    SessionStart,
    SessionFinish,
    Collection,
    CollectionFinish,
    RuntestLogstart,
    RuntestLogfinish,
    RuntestLogreport,
    RuntestCollectreport,
    RuntestWarningRecorded,
}


@pytest.hookimpl()
def pytest_ydist_register_events() -> list[type[Event]]:
    return list(pytest_ydist_events)
