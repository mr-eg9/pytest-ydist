from __future__ import annotations
import abc
from typing import Any, NewType
import enum
import time

import pytest
import collections
from dataclasses import dataclass

@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    session = Session(config)
    config.pluginmanager.register(session, 'dsession')

WorkerId = NewType('WorkerId', int)

class Session:
    """The `Session` instance used by this plugin"""
    def __init__(self, config):
        self.config = config

    @pytest.hookimpl
    def pytest_runtestloop(self, session):
        """Main testloop.

        This procedure forms the backbone of the concurrency model here.
        """
        workers = self.init_workers()
        scheduler = self.init_scheduler(session.items)

        should_reschedule = True
        self.handle_events(workers, scheduler)  # Let scheduler know that workers are populated

        while not scheduler.is_done():
            if should_reschedule:
                should_reschedule = False

                all_idle = self._is_all_workers_idle(workers)
                schedule = scheduler.reschedule(session.items)
                if all_idle and not self._any_new_items_scheduled(schedule):
                    raise RuntimeError('Deadlock, due to no items scheduled when all workers idle')
                self.submit_work(workers, schedule)
            self.wait_for_event()
            should_reschedule |= self.handle_events(workers, scheduler)
            should_reschedule |= self._is_all_workers_idle(workers)

        return True

    # Could be a hook
    def init_workers(self) -> dict[WorkerId, WorkerM]:
        workers = {}
        for i in range(4):
            worker = StupidWorker(self.config)
            workers[WorkerId(i)] = worker
        return workers

    # Could be a hook
    def init_scheduler(self, items) -> Scheduler:
        return StupidScheduler(self.config)

    def wait_for_event(self):
        # TODO: Wait on a multiproccessing.Event, that will be set by the workers
        ...

    def submit_work(self, workers, schedule):
        for worker_idx, worker_schedule in schedule.items():
            for command in worker_schedule:
                workers[worker_idx].submit_command(command)

    def handle_events(self, workers, scheduler) -> bool:
        """Handle incoming events.

        Some event types are handled inside the session, such as `WorkerDied` or `WorkerFinished`.
        All events are passed to the scheduler.

        Return value indicates whether a rescheduling has been requested.
        """
        reschedule = False
        for worker_id, worker in workers.items():
            while (event:=worker.get_event()) is not None:
                event.worker_id = worker_id
                reschedule |= scheduler.notify(event)
        return reschedule

    @staticmethod
    def _is_all_workers_idle(workers):
        return all(worker.is_idle() for worker in workers.values())

    @staticmethod
    def _any_new_items_scheduled(schedule) -> bool:
        for worker_schedule in schedule.values():
            if any(command.status == CommandStatus.Created for command in worker_schedule ):
                return True
        return False

class WorkerM(abc.ABC):
    """A worker that is capable of running tests.

    Should have control functions to allow control of the test execution.
    """
    @abc.abstractmethod
    def __init__(self, config):
        pass

    @abc.abstractmethod
    def submit_command(self, command: Command):
        pass

    @abc.abstractmethod
    def shutdown(self):
        pass

    @abc.abstractmethod
    def is_idle(self) -> bool:
        pass


class Scheduler(abc.ABC):
    @abc.abstractmethod
    def notify(self, event) -> bool:
        """Notify the scheduler an event generated in workers.

        Return value is used to notify the Session that the scheduler wishes to reschedule.
        """
        pass

    @abc.abstractmethod
    def is_done(self) -> bool:
        """Determine if the scheduler believes test execution is complete."""
        ...

    @abc.abstractmethod
    def reschedule(self, items) -> dict:
        """Request that the scheduler (re)schedules the test execution.

        This will be called by the session in case of a worker crash, or if the scheduler
        requests a rescheduling in the `notify` method.

        Returns a `Schedule` which defines how the tests are to be distributed between nodes.
        """
        ...


class StupidWorker(WorkerM):

    def __init__(self, config):
        self.config = config
        self.events = collections.deque()
        self.events.append(WorkerStarted(id))
        self.schedule = []
        self.pending_test = None
        self.next_sched_seq_nr = 0
        self.next_completed_seq_nr = 0
        self._shutdown = False

    def submit_command(self, command: Command):
        assert command.seq_nr == self.next_sched_seq_nr
        assert not self._shutdown
        self.schedule.append(command)
        self.next_sched_seq_nr += 1

        if isinstance(command, RuntestCommand):

            # NOTE: Concurrent scheduler would transmit the command here, and the rest of this
            #  function would typically run on separate thread/ps

            command.status = CommandStatus.Pending
            if len(command.tests) == 0:
                return
            if self.pending_test is not None:
                self._exec_test(self.pending_test, command.tests[0])
            for i in range(len(command.tests) - 1):
                self._exec_test(command.tests[i], command.tests[i+1])
            self.pending_test = command.tests[-1]
            self.events.append(CommandComplete(None, command))
            command.status = CommandStatus.Completed
        if isinstance(command, ShutdownCommand):
            command.status = CommandStatus.Pending
            if self.pending_test is not None:
                self._exec_test(self.pending_test, None)
            self.events.append(CommandComplete(None, command))
            command.status = CommandStatus.Completed
            self.shutdown()

    def get_event(self) -> Event | None:
        if len(self.events) == 0:
            return None
        event = self.events.popleft()

        if isinstance(event, CommandComplete):
            assert event.command == self.schedule[self.next_completed_seq_nr]
            self.next_completed_seq_nr += 1
        return event

    def shutdown(self):
        self._shutdown = True
        self.events.append(WorkerShutdown(None))

    def is_idle(self) -> bool:
        all_commands_completed = self.next_sched_seq_nr == self.next_completed_seq_nr
        all_events_read = len(self.events) == 0
        return all_events_read and all_commands_completed

    def _exec_test(self, test, test_next):
        self.config.hook.pytest_runtest_protocol(item=test, nextitem=test_next)
        self.events.append(TestComplete(None, test))

class StupidScheduler(Scheduler):

    def __init__(self, config):
        self.config = config
        self.remaining_tests = []
        self.workers = []
        self.worker_curr_seq_nr = {}
        self.started = False

    def is_done(self) -> bool:
        return self.started and len(self.remaining_tests) == 0

    def reschedule(self, items) -> dict:
        if not self.started:
            self.started = True
            self.remaining_tests = items
        if not self.workers:
            return {}
        schedule = {worker: [self._shutdown_command(worker)] for worker in self.workers[1:]}
        first_worker = self.workers[0]
        schedule[first_worker] = [  # type: ignore
            self._runtest_command(first_worker, self.remaining_tests),
            self._shutdown_command(first_worker)
        ]
        return schedule

    def notify(self, event) -> bool:
        print(f'received event: {event}')
        reschedule = False
        worker_id = event.worker_id

        if isinstance(event, WorkerStarted):
            self.workers.append(worker_id)
            self.worker_curr_seq_nr[worker_id] = 0
            reschedule = True

        if isinstance(event, WorkerShutdown):
            self.workers.remove(event.worker_id)
            self.worker_curr_seq_nr.pop(event.worker_id)

        if isinstance(event, TestComplete):
            self.remaining_tests.remove(event.test_id)

        return reschedule

    def _runtest_command(self, worker, tests):
        command = RuntestCommand(
            worker,
            self._get_next_seq_nr(worker),
            CommandStatus.Created,
            tests,
        )
        return command

    def _shutdown_command(self, worker):
        command = ShutdownCommand(
            worker,
            self._get_next_seq_nr(worker),
            CommandStatus.Created
        )
        return command

    def _get_next_seq_nr(self, worker):
        seq_nr = self.worker_curr_seq_nr[worker]
        self.worker_curr_seq_nr[worker] = seq_nr + 1
        return seq_nr

@dataclass
class Event(abc.ABC):
    worker_id: Any

@dataclass
class WorkerStarted(Event):
    worker_id: Any

@dataclass
class WorkerShutdown(Event):
    worker_id: Any

@dataclass
class TestComplete(Event):
    worker_id: Any
    test_id: Any

@dataclass
class CommandComplete(Event):
    command: Command

@dataclass
class Command(abc.ABC):
    worker_id: WorkerId
    seq_nr: int
    status: CommandStatus

@dataclass
class ShutdownCommand(Command):
    worker_id: WorkerId
    seq_nr: int
    status: CommandStatus

@dataclass
class RuntestCommand(Command):
    worker_id: WorkerId
    seq_nr: int
    status: CommandStatus
    tests: list[int]

class CommandStatus(enum.Enum):
    Created = enum.auto()
    """Command is created, but not yet submitted to a worker."""
    Pending = enum.auto()
    """Command is either in queue or in progress."""
    Canceled = enum.auto()
    """Command was never executed, and never will be."""
    Aborted = enum.auto()
    """Command was started, but did not complete."""
    Completed = enum.auto()
    """Command successfully completed."""
