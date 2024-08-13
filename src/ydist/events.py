from __future__ import annotations

import pytest
from dataclasses import dataclass, asdict
from typing import Any

from ydist.types import SeqNr, CommandStatus, Event, TestIdx


@dataclass
class CommandChangedStatus(Event):
    seq_nr: SeqNr
    new_status: CommandStatus


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
class CustomEvent(Event):
    data: Any

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
    warning_message: SerializedWarningMessage
    when: str
    nodeid: str
    location: tuple[str, int, str] | None

@dataclass
class SerializedWarningMessage:
    message: str
    category: str # I believe this is actuall the `__class__` of the warning, might need to remake this
    filename: str
    lineno: int
    source: str


# All events that are part of ydist
pytest_ydist_events = {
    CommandChangedStatus,
    WorkerStarted,
    WorkerShutdown,
    TestComplete,
    CustomEvent,
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
def pytest_ydist_event_to_serializable(event: Event) -> dict | None:
    if event.__class__ in pytest_ydist_events:
        event_data = asdict(event)
        event_data['kind'] = event.__class__.__name__
        if isinstance(event, CommandChangedStatus):
            event_data['new_status'] = event_data['new_status'].name
        return event_data


@pytest.hookimpl()
def pytest_ydist_event_from_serializable(event_data: dict) -> Event | None:
    kind = event_data.pop('kind')
    event_cls = next((e for e in pytest_ydist_events if e.__name__ == kind), None)
    if event_cls is None:
        return
    if event_cls is CommandChangedStatus:
        event_data['new_status'] = CommandStatus[event_data['new_status']]
    return event_cls(**event_data)
