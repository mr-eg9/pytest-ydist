from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, NewType, Type
import abc
import enum
from collections import deque

import pytest

# ------------------------------------------------------------------------------
# Main objects
# ------------------------------------------------------------------------------

@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    session = Session(config)
    config.pluginmanager.register(session, 'dsession')

class ScheduleTracker:
    """Utility object to help a scheduler keep track of and schedule commands."""
    def __init__(self):
        self.worker_commands: dict[WorkerId, deque[Command]] = {}
        self.worker_next_seq_nr: dict[WorkerId, SeqNr] = {}

    def add_worker(self, worker_id: WorkerId):
        assert worker_id not in self.worker_commands
        assert worker_id not in self.worker_next_seq_nr
        self.worker_commands[worker_id] = deque()
        self.worker_next_seq_nr[worker_id] = SeqNr(0)

    def remove_worker(self, worker_id: WorkerId) -> deque[Command]:
        assert worker_id in self.worker_commands
        return self.worker_commands.pop(worker_id)

    def worker_ids(self) -> Iterable[WorkerId]:
        return self.worker_commands.keys()

    def get_commands(self, worker) -> deque[Command]:
        return self.worker_commands[worker]

    def clear_command(self, worker_id: WorkerId, seq_nr: SeqNr):
        idx = self.get_command_idx(worker_id, seq_nr)
        del self.worker_commands[worker_id][idx]

    def update_command_status(self, worker_id: WorkerId, seq_nr: SeqNr, status: CommandStatus):
        idx = self.get_command_idx(worker_id, seq_nr)
        self.worker_commands[worker_id][idx].status = status

    def schedule_command(
        self,
        schedule: Schedule,
        worker_id: WorkerId,
        command_cls: Type[Command],
        *args,
        **kwargs,
    ) -> SeqNr:
        seq_nr = self.worker_next_seq_nr[worker_id]
        command = command_cls(seq_nr, worker_id, CommandStatus.Pending, *args, **kwargs)
        self.worker_next_seq_nr[worker_id] = SeqNr(seq_nr + 1)
        schedule.new_commands.append(command)
        self.worker_commands[worker_id].append(command)
        return seq_nr

    def schedule_cancelation(self, schedule: Schedule, worker_id, seq_nr: SeqNr, abort: bool):
        # TODO: Should we track reqested cancelations?
        schedule.cancelations.append(Cancelation(worker_id=worker_id, seq_nr=seq_nr, abort=abort))

    def get_command_idx(self, worker_id, seq_nr) -> int:
        for i, command in enumerate(self.worker_commands[worker_id]):
            if command.seq_nr == seq_nr:
                return i
        raise StopIteration(f'Could not find index for command with worker_id {worker_id} seq_nr {seq_nr}')

    def get_command_in_progress(self, worker_id) -> Command | None:
        for command in self.worker_commands[worker_id]:
            if command.status == CommandStatus.InProgress:
                return command


@dataclass
class Schedule:
    new_commands: list[Command] = field(default_factory=lambda: [])
    cancelations: list[Cancelation] = field(default_factory=lambda: [])


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



class Session:
    """The `Session` instance used by this plugin"""
    def __init__(self, config):
        self.config = config

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtestloop(self, session):
        """Main testloop.

        This procedure forms the backbone of the concurrency model here.
        """
        workers = self.init_workers(session, self.config)
        scheduler = self.init_scheduler(session, self.config)
        # Handle initial `WorkerCreated` events
        self.handle_events(workers, scheduler)

        should_reschedule = True

        while not scheduler.is_done():
            if should_reschedule:
                should_reschedule = False

                all_idle = self._is_all_workers_idle(workers)
                schedule = scheduler.reschedule()
                if all_idle and len(schedule.new_commands) == 0:
                    raise RuntimeError('Deadlock, due to no items scheduled when all workers idle')

                self.submit_work(workers, schedule)
            self.wait_for_event()
            should_reschedule |= self.handle_events(workers, scheduler)
            should_reschedule |= self._is_all_workers_idle(workers)

        return True

    # Could be a hook
    def init_workers(self, session, config) -> dict[WorkerId, Worker]:
        return {WorkerId(i): SyncWorker(i, session, config) for i in range(2)}

    # Could be a hook
    def init_scheduler(self, session, config) -> Scheduler:
        return RoundRobinScheduler(session, config)

    def wait_for_event(self):
        # TODO: Wait on a multiproccessing.Event, that will be set by the workers
        ...

    def submit_work(self, workers: dict[WorkerId, Worker], schedule: Schedule):
        for cancellation in schedule.cancelations:
            workers[cancellation.worker_id].submit_cancellation(cancellation)
        for new_command in schedule.new_commands:
            workers[new_command.worker_id].submit_new_command(new_command)

    def handle_events(self, workers: dict[WorkerId, Worker], scheduler: Scheduler) -> bool:
        """Handle incoming events.

        Some event types are handled inside the session, such as `WorkerDied` or `WorkerFinished`.
        All events are passed to the scheduler.

        Return value indicates whether a rescheduling has been requested.
        """
        reschedule = False
        for worker in workers.values():
            while (event:=worker.pop_event()) is not None:
                reschedule |= scheduler.notify(event)
        return reschedule

    @staticmethod
    def _is_all_workers_idle(workers):
        return all(worker.is_idle() for worker in workers.values())

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


class SyncWorker(Worker):
    def __init__(self, id, session, config):
        self.id = id
        self.config = config
        self.items = session.items
        self.events: deque[Event] = deque()
        self.pending_test = None
        self._register_event(WorkerStarted)

    def submit_new_command(self, command: Command):
        self._register_event(CommandChangedStatus, command.seq_nr, CommandStatus.InProgress)
        match command:
            case RunTests():
                self._exec_run_tests(command)
            case ShutdownWorker():
                self._exec_shutdown()
        self._register_event(CommandChangedStatus, command.seq_nr, CommandStatus.Completed)

    def submit_cancellation(self, cancellation: Cancelation):
        _ = cancellation
        # No point in cancelleation here, because this is all synchronous, scheduler will see a
        # `CommandStatus.Completed` which may also happen in asynchronous code
        pass

    def pop_event(self) -> Event | None:
        if self.events:
            return self.events.popleft()

    def is_idle(self) -> bool:
        return True

    def _exec_shutdown(self):
        if self.pending_test is not None:
            self._exec_test(self.pending_test, None)
        self._register_event(WorkerShutdown)

    def _exec_run_tests(self, command: RunTests):
        num_tests = len(command.tests)
        if num_tests == 0:
            return
        if self.pending_test is not None:
            self._exec_test(self.pending_test, command.tests[0])
        for i in range(num_tests - 1):
            self._exec_test(command.tests[i], command.tests[i + 1])
        self.pending_test = command.tests[-1]

    def _exec_test(self, test_idx: TestIdx, test_next_idx: TestIdx | None):
        test = self.items[test_idx]
        test_next = self.items[test_next_idx] if test_next_idx is not None else None
        self.config.hook.pytest_runtest_protocol(item=test, nextitem=test_next)
        self._register_event(TestComplete, test_idx)

    def _register_event(self, event_cls: Type[Event], *args, **kwargs):
        self.events.append(event_cls(self.id, *args, **kwargs))
        # TODO: Set the events flag in session


class RoundRobinScheduler(Scheduler):
    def __init__(self, session, config):
        self.config = config
        self.unscheduled_items = deque(range(len(session.items)))
        self.schedule_tracker = ScheduleTracker()

    def is_done(self) -> bool:
        return len(self.unscheduled_items) == 0 and len(list(self.schedule_tracker.worker_ids())) == 0

    def reschedule(self) -> Schedule:
        schedule = Schedule()

        for worker_id in self.schedule_tracker.worker_ids():
            if len(self.schedule_tracker.get_commands(worker_id)) == 0:
                if len(self.unscheduled_items) == 0:
                    self.schedule_tracker.schedule_command(schedule, worker_id, ShutdownWorker)
                else:
                    pop_count = 1
                    if len(self.unscheduled_items) > pop_count:
                        test_to_run = [self.unscheduled_items.popleft() for _ in range(pop_count)]
                    else:
                        test_to_run = [e for e in self.unscheduled_items]
                        self.unscheduled_items.clear()

                    self.schedule_tracker.schedule_command(schedule, worker_id, RunTests, test_to_run)
        return schedule

    def notify(self, event: Event) -> bool:
        if isinstance(event, WorkerStarted):
            self.schedule_tracker.add_worker(event.worker_id)
            return True

        if isinstance(event, WorkerShutdown):
            remaining_commands = self.schedule_tracker.remove_worker(event.worker_id)
            match remaining_commands:
                case [ShutdownCommand]:
                    pass
                case _:
                    raise RuntimeError('Worker unexpectedly shut down while there were remaining items')
            return False

        if isinstance(event, CommandChangedStatus):
            if event.worker_id not in self.schedule_tracker.worker_ids():
                # This is a shutdown event
                return False
            self.schedule_tracker.update_command_status(event.worker_id, event.seq_nr, event.new_status)
            if event.new_status == CommandStatus.Aborted:
                raise NotImplementedError('Error handling not yet implemented')
            elif event.new_status == CommandStatus.Completed:
                # TODO: We might need to handle the case where we requested to cancel this command
                self.schedule_tracker.clear_command(event.worker_id, event.seq_nr)
                # This is round-robin, so we know there is only 1 command in the queue
                return True

        if isinstance(event, TestComplete):
            pass

        return False

# ------------------------------------------------------------------------------
# Commands
# ------------------------------------------------------------------------------


SeqNr = NewType('SeqNr', int)
WorkerId = NewType('WorkerId', int)
TestIdx = NewType('TestIdx', int)


@dataclass
class Cancelation:
    worker_id: WorkerId
    seq_nr: SeqNr
    abort: bool


@dataclass
class Command(abc.ABC):
    seq_nr: SeqNr
    worker_id: WorkerId
    status: CommandStatus


@dataclass
class RunTests(Command):
    tests: list[TestIdx]


@dataclass
class ShutdownWorker(Command):
    pass


class CommandStatus(enum.Enum):
    Pending = enum.auto()
    InProgress = enum.auto()
    Completed = enum.auto()
    Canceled = enum.auto()
    Aborted = enum.auto()


# ------------------------------------------------------------------------------
# Events
# ------------------------------------------------------------------------------


@dataclass
class Event(abc.ABC):
    worker_id: WorkerId


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
