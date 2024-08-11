from __future__ import annotations

import pytest
from ydist.types import WorkerId, Worker, Schedule, Scheduler

from ydist.workers.psworker import ProccessWorker
from ydist.workers.syncworker import SyncWorker
from ydist.workers.threadworker import ThreadWorker
from ydist.schedulers.round_robin import RoundRobinScheduler
from ydist import commands, events

from threading import Event as MpEvent

class Session:
    """The `Session` instance used by this plugin"""
    def __init__(self, config):
        self.config = config
        self.has_events = MpEvent()

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtestloop(self, session):
        """Main testloop.

        This procedure forms the backbone of the concurrency model here.
        """
        workers = self.init_workers(session, self.config, self.has_events)
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
                    # raise RuntimeError('Deadlock, due to no items scheduled when all workers idle')
                    pass

                self.submit_work(workers, schedule)
            self.wait_for_event()
            should_reschedule |= self.handle_events(workers, scheduler)
            should_reschedule |= self._is_all_workers_idle(workers)

        return True

    # Could be a hook
    def init_workers(self, session, config, has_events) -> dict[WorkerId, Worker]:
        return {WorkerId(i): ProccessWorker(i, session, config, has_events) for i in range(10)}

    def shutdown_workers(self, workers):
        for worker in workers.values():
            worker.submit_new_command(commands.ShutdownWorker)

    # Could be a hook
    def init_scheduler(self, session, config) -> Scheduler:
        return RoundRobinScheduler(session, config)

    def wait_for_event(self):
        self.has_events.wait()
        self.has_events.clear()

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
        workers_to_destroy = set()

        for worker_id, worker in workers.items():
            while (event:=worker.pop_event()) is not None:
                if event == events.WorkerShutdown:
                    workers_to_destroy.add(worker_id)
                reschedule |= scheduler.notify(event)

        for worker_id in workers_to_destroy:
            del workers[worker_id]
        return reschedule

    @staticmethod
    def _is_all_workers_idle(workers):
        return all(worker.is_idle() for worker in workers.values())
