from __future__ import annotations

from typing import Type
from collections import deque
from multiprocessing.synchronize import Event as MpEvent
import threading
import os
import sys

from ydist.types import Worker, Event, Cancelation, TestIdx, CommandStatus, Command
from ydist import commands
from ydist import events


class ThreadWorker(Worker):
    def __init__(self, worker_id, session, config, has_event):
        self.worker_id = worker_id
        self.has_events: MpEvent = has_event
        self.submitted_commands = 0
        self.worker_thread = WorkerThread(worker_id, self.has_events, config, session.items)
        self.worker_thread.daemon = True

        self.worker_thread.start()
        self.worker_thread.has_events.wait()

    def submit_new_command(self, command: Command):
        self.worker_thread.submit_command(command)
        self.submitted_commands += 1
        if isinstance(command, commands.ShutdownWorker):
            self.worker_thread.join()

    def submit_cancellation(self, cancellation: Cancelation):
        self.worker_thread.submit_cancellation(cancellation)

    def pop_event(self) -> Event | None:
        if len(self.worker_thread.event_queue) > 0:
            # NOTE: deque.popleft is threadsafe from outside the thread
            return self.worker_thread.event_queue.popleft()

    def is_idle(self) -> bool:
        return self.submitted_commands == self.worker_thread.completed_commands


class WorkerThread(threading.Thread):
    def __init__(self, id, has_events, config, items):
        super().__init__()
        self.id = id
        self.event_queue = deque()
        self.command_queue = deque()
        self.has_commands = threading.Event()
        self.has_events = has_events
        self.config = config
        self.items = items
        self.pending_test: TestIdx | None = None
        self.completed_commands = 0

    def submit_command(self, command):
        # NOTE: deque.append is threadsafe
        self.command_queue.append(command)
        self.has_commands.set()

    def submit_cancellation(self, command):
        # TODO: Implement
        pass

    def run(self):
        self._register_event(events.WorkerStarted)
        while True:
            command = self._pop_command()
            self._register_event(events.CommandChangedStatus, command.seq_nr, CommandStatus.InProgress)
            match command:
                case commands.RunTests():
                    self._exec_run_tests(command)
                case commands.ShutdownWorker():
                    if self.pending_test is not None:
                        self._exec_test(self.pending_test, None)
                    break
            self._register_event(events.CommandChangedStatus, command.seq_nr, CommandStatus.Completed)
            # Not super threadsafe, but there is only single-mutation here and `is_idle` does
            #  not have to be perfectly accurate
            self.completed_commands += 1
        self._register_event(events.WorkerShutdown)

    def _pop_command(self) -> Command:
        # NOTE: deque.popleft is threadsafe from inside the thread
        self.has_commands.wait()
        self.has_commands.clear()
        command =  self.command_queue.popleft()
        if len(self.command_queue) > 0:
            self.has_commands.set()
        return command

    def _register_event(self, event_cls: Type[Event], *args, **kwargs):
        self.event_queue.append(event_cls(self.id, *args, **kwargs))
        self.has_events.set()

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
