from __future__ import annotations

from typing import Type
from collections import deque

from ydist.types import Worker, Event, Cancelation, TestIdx, CommandStatus, Command
from ydist import commands
from ydist import events

class SyncWorker(Worker):
    def __init__(self, id, session, config):
        self.id = id
        self.config = config
        self.items = session.items
        self.events: deque[Event] = deque()
        self.pending_test = None
        self._register_event(events.WorkerStarted)

    def submit_new_command(self, command: Command):
        self._register_event(events.CommandChangedStatus, command.seq_nr, CommandStatus.InProgress)
        match command:
            case commands.RunTests():
                self._exec_run_tests(command)
            case commands.ShutdownWorker():
                self._exec_shutdown()
        self._register_event(events.CommandChangedStatus, command.seq_nr, CommandStatus.Completed)

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
        self._register_event(events.WorkerShutdown)

    def _exec_run_tests(self, command: commands.RunTests):
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
        self._register_event(events.TestComplete, test_idx)

    def _register_event(self, event_cls: Type[Event], *args, **kwargs):
        self.events.append(event_cls(self.id, *args, **kwargs))
        # TODO: Set the events flag in session
