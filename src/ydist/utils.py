from __future__ import annotations

from typing import Type, Iterable
from collections import deque

from ydist.types import WorkerId, Command, SeqNr, CommandStatus, Schedule, Cancelation

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

