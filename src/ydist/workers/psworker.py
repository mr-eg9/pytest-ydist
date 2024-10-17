from __future__ import annotations
from dataclasses import dataclass

import pytest

from typing import Any, Generator, Type
from collections import deque
import threading
import subprocess
import sys
import socket
import json
import warnings
import importlib

from ydist.metacommands import WorkerMetaCommand
from ydist.types import EventSender, MetaCommand, Worker, Event, TestIdx, CommandStatus, Command
from ydist import commands
from ydist import events


class ProccessWorker(Worker):
    def __init__(
        self,
        worker_id,
        session,
        config,
        has_events: threading.Event
    ):
        _ = session
        self.worker_id = worker_id
        self.config = config
        self.has_event = has_events
        self.json_encoder = json.JSONEncoder()
        self.completed_commands = 0
        self.submitted_commands = 0
        self.session = session

        worker_socket = socket.create_server(('localhost', 0))  # Let the OS pick the port
        worker_socket_addr = f'localhost:{worker_socket.getsockname()[1]}'
        argv = sys.argv.copy()
        argv.extend(['--ydist-worker-addr', worker_socket_addr])
        argv.extend(['--ydist-worker-id', str(self.worker_id)])
        sub_out = subprocess.DEVNULL
        # sub_out = None
        self.worker_ps = self.worker_ps = subprocess.Popen(argv, stdout=sub_out)
        self.conn, _ = worker_socket.accept()

        self.event_receiver = EventReceiver(config, worker_id, has_events, self.conn)
        self.event_receiver.start()


    def submit_new_command(self, command: Command):
        command_data = command.to_serializable()
        command_data['kind'] = command.__class__.__qualname__
        try:
            json_data = self.json_encoder.encode(command_data)
        except TypeError as e:
            raise TypeError(f'Failed to turn {command} into a serializable object') from e
        self.conn.send(bytes(json_data, 'utf-8'))
        self.submitted_commands += 1

    def submit_new_metacommand(self, metacommand: WorkerMetaCommand):
        raise NotImplementedError('Worker metacommands are not yet implemented')

    def terminate(self):
        self.conn.close()
        self.worker_ps.terminate()

    def pop_event(self) -> Event | None:
        if len(self.event_receiver.event_queue) > 0:
            # NOTE: deque.popleft is threadsafe from outside the thread
            event = self.event_receiver.event_queue.popleft()
            self._handle_worker_events(event)
            return event

    def is_idle(self) -> bool:
        return self.completed_commands == self.submitted_commands

    def _handle_worker_events(self, event: Event) -> None:
        match event:
            case events.CommandChangedStatus(new_status=CommandStatus.Completed):
                self.completed_commands += 1
            case events.WorkerShutdown():
                self.event_receiver.should_shutdown = True
                self.conn.close()
            case events.RuntestLogstart():
                self.config.hook.pytest_runtest_logstart(nodeid=event.nodeid, location=event.location)
            case events.RuntestLogfinish():
                self.config.hook.pytest_runtest_logfinish(nodeid=event.nodeid, location=event.location)
            case events.RuntestLogreport():
                report = self.config.hook.pytest_report_from_serializable(config=self.config, data=event.report)
                self.config.hook.pytest_runtest_logreport(report=report)
            case events.RuntestCollectreport():
                report = self.config.hook.pytest_report_from_serializable(config=self.config, data=event.report)
                self.config.hook.pytest_collectreport(report=report)
            case events.RuntestWarningRecorded():
                try:
                    category_qualname = event.warning_message.category
                    mod_qualname, cls = category_qualname.rsplit('.', 1)
                    package, module = mod_qualname.rsplit('.', 1)
                    mod = importlib.import_module(module, package)
                    category: Type[Warning] = getattr(mod, cls)
                except ImportError:
                    category = Warning
                warning_message = warnings.WarningMessage(
                    event.warning_message.message,
                    category,
                    event.warning_message.filename,
                    event.warning_message.lineno
                )
                self.config.hook.pytest_runtest_warning_recorded(
                    warning_message,
                    event.when,
                    event.nodeid,
                    event.location
                )


class EventReceiver(threading.Thread):
    def __init__(
        self,
        config: pytest.Config,
        worker_id,
        has_events,
        conn: socket.socket
    ):
        super().__init__()
        self.config = config
        self.worker_id = worker_id
        self.has_events = has_events
        self.event_queue = deque()
        self.should_shutdown = False
        self.conn = conn
        self.data_buffer = ''
        self.json_decoder = json.JSONDecoder()
        self.event_types = {
            event_cls.__qualname__: event_cls
            for registered_event_types in self.config.hook.pytest_ydist_register_events()
            for event_cls in registered_event_types
        }

    def run(self):
        while (event_data := self._read_event_json()) is not None:
            kind = event_data.pop('kind')
            event_cls: Event = self.event_types[kind]
            event = event_cls.from_serializable(event_data)
            assert event is not None, f'Failed to decode event_data: {event_data}'
            self.event_queue.append(event)
            self.has_events.set()

    def _read_event_json(self) -> dict | None:
        while not self.should_shutdown:
            try:
                event_data, len = self.json_decoder.raw_decode(self.data_buffer)
                self.data_buffer = self.data_buffer[len:]
                return event_data
            except json.JSONDecodeError:
                try:
                    new_data = self.conn.recv(1024)
                except ConnectionResetError:
                    return None
                if new_data == b'':
                    return None
                self.data_buffer += new_data.decode('utf-8')

    def _register_event(self, event_cls: Type[Event], *args, **kwargs):
        self.event_queue.append(event_cls(self.worker_id, *args, **kwargs))
        self.has_events.set()


class WorkerEventSender(EventSender):
    def __init__(self, config: pytest.Config, conn: socket.socket):
        self.config = config
        self.json_encoder = json.JSONEncoder()
        self.conn = conn

    def send(self, event: Event):
        """Send an event from this worker.

        This is the only mandatory part of the `WorkerSession` API.
        """
        event_data = event.to_serializable()
        event_data['kind'] = event.__class__.__qualname__
        json_data = self.json_encoder.encode(event_data)
        self.conn.send(bytes(json_data, 'utf-8'))


class WorkerProccess:
    def __init__(self, config):
        self.config = config
        self.worker_id = config.getvalue('ydist_worker_id')
        worker_socket_addr = config.getvalue('ydist_worker_addr')
        addr, port = worker_socket_addr.split(':')
        self.conn = socket.create_connection((addr, int(port)))
        self.event_sender = WorkerEventSender(config, self.conn)
        self.json_decoder = json.JSONDecoder()
        self.should_shutdown = False
        self.command_data_buffer = ''
        self.pending_test = None
        self.command_types = {
            command_cls.__qualname__: command_cls
            for registered_command_types in self.config.hook.pytest_ydist_register_commands()
            for command_cls in registered_command_types
        }
        self.metacommand_types = {
            metacommand_cls.__qualname__: metacommand_cls
            for registered_metacommand_types in self.config.hook.pytest_ydist_register_metacommands()
            for metacommand_cls in registered_metacommand_types
        }


    @pytest.hookimpl(tryfirst=True)
    def pytest_runtestloop(self, session):
        self.items = session.items
        self._send_event(events.WorkerStarted)
        while not self.should_shutdown:
            command_data = self._read_command_data()
            if command_data is None:
                continue

            # TODO: All metacommands should be handled before any normal commands are,
            #  or metacommands should be executed in a separate thread
            if command_data.get('is_meta', False):
                command_data.pop('is_meta')
                kind = command_data.pop('kind')
                metacommand_cls = self.metacommand_types[kind]
                metacommand = metacommand_cls.from_serializable(command_data)
                self.config.hook.pytest_worker_handle_metacommand(metacommand)
            else:
                kind = command_data.pop('kind')
                command_cls = self.command_types[kind]
                command = command_cls.from_serializable(command_data)
                self._send_command_changed_status(command, CommandStatus.InProgress)
                self.config.hook.pytest_worker_handle_command(
                    config=self.config,
                    command=command,
                    event_sender=self.event_sender,
                )
                self._send_command_changed_status(command, CommandStatus.Completed)

        self._send_event(events.WorkerShutdown)
        return True


    @pytest.hookimpl()
    def pytest_worker_handle_command(
        self,
        config: pytest.Config,
        command: Command,
        event_sender: EventSender,
    ):
        _ = config, event_sender
        match command:
            case commands.ShutdownWorker():
                self.should_shutdown = True
                if self.pending_test is not None:
                    self._exec_test(self.pending_test, None)
            case commands.RunTests():
                self._exec_run_tests(command)
            case commands.RunPendingTest():
                # assert self.pending_test is not None, 'Tried to run the pending test, but this test does not exist'
                if self.pending_test is None:
                    return
                self._exec_test(self.pending_test, None)
                self.pending_test = None

    @pytest.hookimpl()
    def pytest_worker_handle_metacommand(
        self,
        config: pytest.Config,
        metacommand: WorkerMetaCommand,
        event_sender: EventSender,
    ):
        raise NotImplementedError('Metacommands not yet implemented for this worker')

    def _send_command_changed_status(self, command, new_status):
        self._send_event(
            events.CommandChangedStatus,
            seq_nr=command.seq_nr,
            new_status=new_status,
        )


    def _send_event(self, event_cls: Type[Event], **kwargs):
        event = event_cls(worker_id=self.worker_id, **kwargs)
        self.event_sender.send(event)


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
        self._send_event(events.TestComplete, test_idx=test_idx)

    def _read_command_data(self) -> dict | None:
        try:
            command_data, len = self.json_decoder.raw_decode(self.command_data_buffer)
            self.command_data_buffer = self.command_data_buffer[len:]
            return command_data
        except json.JSONDecodeError:
            try:
                new_data = self.conn.recv(1024)
            except ConnectionResetError:
                return None
            if new_data == b'':
                return None
            self.command_data_buffer += new_data.decode('utf-8')


    @pytest.hookimpl
    def pytest_internalerror(self, excrepr: object, excinfo: object) -> None:
        raise NotImplementedError(f'Handling internal errors not yet supported: {excrepr}, {excinfo}')
        excrepr = str(excrepr)
        self._send_event(Internalerror, excrepr=excrepr)

    @pytest.hookimpl
    def pytest_sessionstart(self, session: pytest.Session) -> None:
        self._send_event(events.SessionStart)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_sessionfinish(self, exitstatus: int) -> Generator[None, object, None]:
        yield
        self._send_event(events.SessionFinish, exitstatus=exitstatus)

    @pytest.hookimpl
    def pytest_collection(self) -> None:
        self._send_event(events.CollectionFinish)

    @pytest.hookimpl
    def pytest_runtest_logstart(
        self,
        nodeid: str,
        location: tuple[str, int | None, str],
    ) -> None:
        self._send_event(events.RuntestLogstart, nodeid=nodeid, location=location)

    @pytest.hookimpl
    def pytest_runtest_logfinish(
        self,
        nodeid: str,
        location: tuple[str, int | None, str],
    ) -> None:
        self._send_event(events.RuntestLogfinish, nodeid=nodeid, location=location)

    @pytest.hookimpl
    def pytest_warning_recorded(
        self,
        warning_message: warnings.WarningMessage,
        when: str,
        nodeid: str,
        location: tuple[str, int, str] | None,
    ) -> None:
        warning_message_ser = events.SerializedWarningMessage(
            str(warning_message.message),
            warning_message.category.__qualname__,
            warning_message.filename,
            warning_message.lineno,
            str(warning_message.source),
        )
        self._send_event(
            events.RuntestWarningRecorded,
            warning_message=warning_message_ser,
            when=when,
            nodeid=nodeid,
            location=location,
        )

    @pytest.hookimpl
    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        report = self.config.hook.pytest_report_to_serializable(
            config=self.config,
            report=report,
        )
        self._send_event(events.RuntestLogreport, report=report)

    @pytest.hookimpl
    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        report = self.config.hook.pytest_report_to_serializable(
            config=self.config,
            report=report,
        )
        self._send_event(events.RuntestCollectreport, report=report)
