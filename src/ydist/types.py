from __future__ import annotations

from dataclasses import dataclass, field
from typing import NewType, TYPE_CHECKING
import abc
import enum


if TYPE_CHECKING:
    from ydist.metacommands import WorkerMetaCommand


SeqNr = NewType('SeqNr', int)
MetaSeqNr = NewType('MetaSeqNr', int)
WorkerId = NewType('WorkerId', int)
TestIdx = NewType('TestIdx', int)


@dataclass
class Schedule:
    new_commands: list[Command] = field(default_factory=lambda: [])
    new_metacommands: list[MetaCommand] = field(default_factory=lambda: [])


@dataclass
class Event(abc.ABC):
    worker_id: WorkerId


@dataclass
class Command(abc.ABC):
    seq_nr: SeqNr | None  # May be None, in the case where this command is embedded in another command
    worker_id: WorkerId
    status: CommandStatus


@dataclass
class MetaCommand(abc.ABC):
    meta_seq_nr: MetaSeqNr
    meta_target: MetaTarget


class CommandStatus(enum.Enum):
    Pending = enum.auto()
    InProgress = enum.auto()
    Completed = enum.auto()
    Canceled = enum.auto()
    Aborted = enum.auto()


class MetaTarget(enum.Enum):
    Session = enum.auto()
    Worker = enum.auto()


class Worker(abc.ABC):
    @abc.abstractmethod
    def __init__(self, worker_id, config, items, has_event):
        pass

    @abc.abstractmethod
    def submit_new_command(self, command: Command):
        pass

    @abc.abstractmethod
    def submit_new_metacommand(self, metacommand: WorkerMetaCommand):
        pass

    @abc.abstractmethod
    def pop_event(self) -> Event | None:
        pass

    @abc.abstractmethod
    def is_idle(self) -> bool:
        pass


class Scheduler(abc.ABC):
    @abc.abstractmethod
    def __init__(self, session, config):
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

