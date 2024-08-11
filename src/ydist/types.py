from __future__ import annotations

from dataclasses import dataclass, field
from typing import NewType
import abc
import enum


SeqNr = NewType('SeqNr', int)
WorkerId = NewType('WorkerId', int)
TestIdx = NewType('TestIdx', int)


@dataclass
class Cancelation:
    worker_id: WorkerId
    seq_nr: SeqNr
    abort: bool


@dataclass
class Schedule:
    new_commands: list[Command] = field(default_factory=lambda: [])
    cancelations: list[Cancelation] = field(default_factory=lambda: [])


@dataclass
class Event(abc.ABC):
    worker_id: WorkerId


@dataclass
class Command(abc.ABC):
    seq_nr: SeqNr
    worker_id: WorkerId
    status: CommandStatus


class CommandStatus(enum.Enum):
    Pending = enum.auto()
    InProgress = enum.auto()
    Completed = enum.auto()
    Canceled = enum.auto()
    Aborted = enum.auto()


class Worker(abc.ABC):
    @abc.abstractmethod
    def __init__(self, id, config, items):
        pass

    @abc.abstractmethod
    def submit_new_command(self, command: Command):
        pass

    @abc.abstractmethod
    def submit_cancellation(self, cancellation: Cancelation):
        pass

    @abc.abstractmethod
    def pop_event(self) -> Event | None:
        pass

    @abc.abstractmethod
    def is_idle(self) -> bool:
        pass


class Scheduler(abc.ABC):
    @abc.abstractmethod
    def __init__(self, items, workers):
        pass

    @abc.abstractmethod
    def is_done(self) -> bool:
        pass

    @abc.abstractmethod
    def reschedule(self) -> Schedule:
        pass

    @abc.abstractmethod
    def notify(self, event: Event) -> bool:
        pass

