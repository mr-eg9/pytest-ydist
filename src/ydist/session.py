from __future__ import annotations

import pytest
from ydist.types import WorkerId, Worker, Schedule, Scheduler

from ydist.workers.syncworker import SyncWorker
from ydist.schedulers.round_robin import RoundRobinScheduler

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
