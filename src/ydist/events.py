from __future__ import annotations

from dataclasses import dataclass

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
