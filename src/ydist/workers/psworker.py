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

from ydist.types import Worker, Event, Cancelation, TestIdx, CommandStatus, Command
from ydist import commands
from ydist import events


class ProccessWorker(Worker):
    def __init__(self, worker_id, session, config, has_events: threading.Event):
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

        self.event_receiver = EventReceiver(worker_id, has_events, self.conn)
        self.event_receiver.start()


    def submit_new_command(self, command: Command):
        command_data = self._encode_command(command)
        self.conn.send(bytes(command_data, 'utf-8'))
        self.submitted_commands += 1

    def submit_cancellation(self, cancellation: Cancelation):
        pass

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
            # TODO: Implement
            # case Internalerror():
            #     self.config.hook.pytest_internalerror(event.excrepr)
            case SessionStart():
                pass
                # self.config.hook.pytest_sessionstart()
            case SessionFinish():
                self.event_receiver.should_shutdown = True
                self.conn.close()
                # self.config.hook.pytest_sessionfinish(self.session, event.exitstatus)
            case Collection():
                # TODO: Figure out what to do about this one
                pass # This is just a wrapper around Collection, to indicate where collection starts
                # self.config.hook.collection()
            case CollectionFinish():
                # self.config.hook.pytest_collection_finish(self.session)
                pass
            case RuntestLogstart():
                self.config.hook.pytest_runtest_logstart(nodeid=event.nodeid, location=event.location)
            case RuntestLogfinish():
                self.config.hook.pytest_runtest_logfinish(nodeid=event.nodeid, location=event.location)
            case RuntestLogreport():
                report = self.config.hook.pytest_report_from_serializable(config=self.config, data=event.report)
                self.config.hook.pytest_runtest_logreport(report=report)
            case RuntestCollectreport():
                report = self.config.hook.pytest_report_from_serializable(config=self.config, data=event.report)
                self.config.hook.pytest_collectreport(report=report)
            case RuntestWarningRecorded():
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

    def _encode_command(self, command: Command):
        command_data = {
            'kind': command.__class__.__name__,
            'seq_nr': command.seq_nr,
            'worker_id': command.worker_id,
            'status': command.status.name,
        }
        match command:
            case commands.RunTests():
                command_data['tests'] = command.tests
                return self.json_encoder.encode(command_data)
            case commands.ShutdownWorker():
                return self.json_encoder.encode(command_data)
        raise NotImplementedError(f'Unable to encode command {command}')


class EventReceiver(threading.Thread):
    def __init__(self, id, has_events, conn: socket.socket):
        super().__init__()
        self.worker_id = id
        self.has_events = has_events
        self.event_queue = deque()
        self.should_shutdown = False
        self.conn = conn
        self.data_buffer = ''
        self.json_decoder = json.JSONDecoder()

    def run(self):
        while (event_data := self._read_event_json()) is not None:
            event = self._decode_event(event_data)
            self.event_queue.append(event)
            self.has_events.set()

    def _read_event_json(self) -> dict | None:
        while not self.should_shutdown:
            try:
                event_data, len = self.json_decoder.raw_decode(self.data_buffer)
                self.data_buffer = self.data_buffer[len:]
                return event_data
            except json.JSONDecodeError:
                new_data = self.conn.recv(1024)
                if new_data == b'':
                    return None
                self.data_buffer += new_data.decode('utf-8')

    def _decode_event(self, event_data: dict) -> Event:
        assert event_data['worker_id'] == self.worker_id
        assert 'kind' in event_data
        kind = event_data.pop('kind')
        match kind:
            case 'WorkerStarted':
                return events.WorkerStarted(**event_data)
            case 'WorkerShutdown':
                self.should_shutdown = True
                return events.WorkerShutdown(**event_data)
            case 'TestComplete':
                return events.TestComplete(**event_data)
            case 'CommandChangedStatus':
                event_data['new_status'] = CommandStatus[event_data['new_status']]
                return events.CommandChangedStatus(**event_data)
            # case 'Internalerror':
            #     return Internalerror(**event_data)
            case 'SessionStart':
                return SessionStart(**event_data)
            case 'SessionFinish':
                return SessionFinish(**event_data)
            case 'Collection':
                return Collection(**event_data)
            case 'CollectionFinish':
                return CollectionFinish(**event_data)
            case 'RuntestLogstart':
                return RuntestLogstart(**event_data)
            case 'RuntestLogfinish':
                return RuntestLogfinish(**event_data)
            case 'RuntestLogreport':
                return RuntestLogreport(**event_data)
            case 'RuntestCollectreport':
                return RuntestCollectreport(**event_data)
            case 'RuntestWarningRecorded':
                return RuntestWarningRecorded(**event_data)
        # TODO: Should probably call a hook here, rather than raising an exception?
        raise NotImplementedError(f'Unknown event {event_data}')

    def _register_event(self, event_cls: Type[Event], *args, **kwargs):
        self.event_queue.append(event_cls(self.worker_id, *args, **kwargs))
        self.has_events.set()


class WorkerProccess:
    def __init__(self, config):
        self.config = config
        self.worker_id = config.getvalue('ydist_worker_id')
        worker_socket_addr = config.getvalue('ydist_worker_addr')
        addr, port = worker_socket_addr.split(':')
        self.conn = socket.create_connection((addr, int(port)))
        self.json_encoder = json.JSONEncoder()
        self.json_decoder = json.JSONDecoder()
        self.should_shutdown = False
        self.command_data_buffer = ''
        self.pending_test = None

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtestloop(self, session):
        self.items = session.items
        self._send_event(events.WorkerStarted)
        while not self.should_shutdown:
            command_data = self._read_command_data()
            command = self._decode_command(command_data)
            self._send_event(
                events.CommandChangedStatus,
                worker_id=command.worker_id,
                seq_nr=command.seq_nr,
                new_status=CommandStatus.InProgress,
            )

            match command:
                case commands.ShutdownWorker():
                    self.should_shutdown = True
                    if self.pending_test is not None:
                        self._exec_test(self.pending_test, None)
                case commands.RunTests():
                    self._exec_run_tests(command)

            self._send_event(
                events.CommandChangedStatus,
                worker_id=command.worker_id,
                seq_nr=command.seq_nr,
                new_status=CommandStatus.Completed,
            )
        self._send_event(events.WorkerShutdown)
        return True

    def _send_event(self, event_cls: Type[Event], **kwargs):
        data = {
            'kind': event_cls.__name__,
            'worker_id': self.worker_id,
            **kwargs,
        }
        if event_cls == events.CommandChangedStatus:
            data['new_status'] = data['new_status'].name
        json_data = self.json_encoder.encode(data)
        self.conn.send(bytes(json_data, 'utf-8'))


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
        while not self.should_shutdown:
            try:
                command_data, len = self.json_decoder.raw_decode(self.command_data_buffer)
                self.command_data_buffer = self.command_data_buffer[len:]
                return command_data
            except json.JSONDecodeError:
                new_data = self.conn.recv(1024)
                if new_data == b'':
                    return None
                self.command_data_buffer += new_data.decode('utf-8')

    def _decode_command(self, command_data) -> Command:
        match command_data['kind']:
            case 'ShutdownWorker':
                return commands.ShutdownWorker(
                    seq_nr=command_data['seq_nr'],
                    worker_id=command_data['worker_id'],
                    status=command_data['status'],
                )
            case 'RunTests':
                return commands.RunTests(
                    seq_nr=command_data['seq_nr'],
                    worker_id=command_data['worker_id'],
                    status=CommandStatus[command_data['status']],
                    tests=command_data['tests'],
                )
        raise NotImplementedError(f'Unknown command {command_data}')

    @pytest.hookimpl
    def pytest_internalerror(self, excrepr: object, excinfo: object) -> None:
        raise NotImplementedError(f'Handling internal errors not yet supported: {excrepr}, {excinfo}')
        excrepr = str(excrepr)
        self._send_event(Internalerror, excrepr=excrepr)

    @pytest.hookimpl
    def pytest_sessionstart(self, session: pytest.Session) -> None:
        self._send_event(SessionStart)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_sessionfinish(self, exitstatus: int) -> Generator[None, object, None]:
        yield
        self._send_event(SessionFinish, exitstatus=exitstatus)

    @pytest.hookimpl
    def pytest_collection(self) -> None:
        self._send_event(CollectionFinish)

    @pytest.hookimpl
    def pytest_runtest_logstart(
        self,
        nodeid: str,
        location: tuple[str, int | None, str],
    ) -> None:
        self._send_event(RuntestLogstart, nodeid=nodeid, location=location)

    @pytest.hookimpl
    def pytest_runtest_logfinish(
        self,
        nodeid: str,
        location: tuple[str, int | None, str],
    ) -> None:
        self._send_event(RuntestLogfinish, nodeid=nodeid, location=location)

    @pytest.hookimpl
    def pytest_warning_recorded(
        self,
        warning_message: warnings.WarningMessage,
        when: str,
        nodeid: str,
        location: tuple[str, int, str] | None,
    ) -> None:
        warning_message_ser = SerializedWarningMessage(
            str(warning_message.message),
            warning_message.category.__qualname__,
            warning_message.filename,
            warning_message.lineno,
            str(warning_message.source),
        )
        self._send_event(
            RuntestWarningRecorded,
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
        self._send_event(RuntestLogreport, report=report)

    @pytest.hookimpl
    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        report = self.config.hook.pytest_report_to_serializable(
            config=self.config,
            report=report,
        )
        self._send_event(RuntestCollectreport, report=report)


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
