from __future__ import annotations

from collections import deque

from ydist.types import Scheduler, Schedule, Event, CommandStatus
from ydist.utils import ScheduleTracker
from ydist import events, commands

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
            command_count = len(self.schedule_tracker.get_commands(worker_id))
            match command_count:
                case 0:
                    self._schedule_tests_for_worker(schedule, worker_id)
                    self._schedule_tests_for_worker(schedule, worker_id)
                case 1:
                    self._schedule_tests_for_worker(schedule, worker_id)
                case _:
                    pass
        return schedule

    def _schedule_tests_for_worker(self, schedule, worker_id):
        if len(self.unscheduled_items) == 0:
            self.schedule_tracker.schedule_command(schedule, worker_id, commands.ShutdownWorker)
        else:
            pop_count = 10
            if len(self.unscheduled_items) > pop_count:
                test_to_run = [self.unscheduled_items.popleft() for _ in range(pop_count)]
            else:
                test_to_run = [e for e in self.unscheduled_items]
                self.unscheduled_items.clear()

            self.schedule_tracker.schedule_command(schedule, worker_id, commands.RunTests, test_to_run)

    def notify(self, event: Event) -> bool:
        if isinstance(event, events.WorkerStarted):
            self.schedule_tracker.add_worker(event.worker_id)
            return True

        if isinstance(event, events.WorkerShutdown):
            remaining_commands = self.schedule_tracker.remove_worker(event.worker_id)
            match remaining_commands:
                case [ShutdownCommand]:
                    pass
                case []:
                    pass
                case _:
                    # TODO: Add better error handling
                    raise RuntimeError('Worker unexpectedly shut down while there were remaining items')
            return False

        if isinstance(event, events.CommandChangedStatus):
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

        if isinstance(event, events.TestComplete):
            # TODO: Handle the case where a command failed/crashed
            pass

        return False
