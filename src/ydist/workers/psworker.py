from __future__ import annotations

import pytest

from typing import Type
from collections import deque
import threading
import subprocess
import sys
import socket
import json

from ydist.types import Worker, Event, Cancelation, TestIdx, CommandStatus, Command
from ydist import commands
from ydist import events


class ProccessWorker(Worker):
    def __init__(self, id, session, config, has_events: threading.Event):
        _ = session
        self.id = id
        self.config = config
        self.has_event = has_events
        self.json_encoder = json.JSONEncoder()
        self.completed_commands = 0
        self.submitted_commands = 0

        worker_socket = socket.create_server(('localhost', 0))  # Let the OS pick the port
        worker_socket_addr = f'localhost:{worker_socket.getsockname()[1]}'
        argv = sys.argv.copy()
        argv.extend(['--ydist-worker-addr', worker_socket_addr])
        self.worker_ps = self.worker_ps = subprocess.Popen(argv)
        self.conn, _ = worker_socket.accept()

        self.event_receiver = EventReceiver(id, has_events, self.conn)
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
            match event:
                case events.CommandChangedStatus(new_status=CommandStatus.Completed):
                    self.completed_commands += 1
            return event

    def is_idle(self) -> bool:
        return self.completed_commands == self.submitted_commands

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
        self.id = id
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

    def _decode_event(self, event_data) -> Event:
        match event_data['kind']:
            case 'WorkerStarted':
                return events.WorkerStarted(self.id)
            case 'WorkerShutdown':
                self.should_shutdown = True
                return events.WorkerShutdown(self.id)
            case 'TestComplete':
                return events.TestComplete(self.id, event_data['test_idx'])
            case 'CommandChangedStatus':
                return events.CommandChangedStatus(
                    self.id,
                    event_data['seq_nr'],
                    CommandStatus[event_data['new_status']],
                )
        # TODO: Should probably call a hook here, rather than raising an exception?
        raise NotImplementedError(f'Unknown event {event_data}')

    def _register_event(self, event_cls: Type[Event], *args, **kwargs):
        self.event_queue.append(event_cls(self.id, *args, **kwargs))
        self.has_events.set()


class WorkerProccess:
    def __init__(self, config):
        self.config = config
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
