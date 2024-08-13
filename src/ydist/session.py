from __future__ import annotations

import pytest
from ydist.metacommands import SessionMetaCommand, WorkerMetaCommand
from ydist.types import WorkerId, Worker, Schedule, Scheduler, Event

from ydist import events
from collections import deque

from threading import Event as MpEvent

class Session:
    """The `Session` instance used by this plugin"""
    def __init__(self, config):

        match (numworkers := config.getvalue('numworkers')):
            case 'auto' | None: self.numworkers = 8
            case _: self.numworkers = int(numworkers)

        self.has_events = MpEvent()
        self.next_worker_id = WorkerId(0)
        self.worker_destroy_queue = deque()

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtestloop(self, session: pytest.Session):
        """Main testloop.

        This procedure forms the backbone of the concurrency model here.
        """

        config: pytest.Config = session.config
        workers: dict[WorkerId, Worker] = {}

        for _ in range(self.numworkers):
            self._add_new_worker(config, workers, session)

        scheduler = config.hook.pytest_ydist_setup_scheduler(
            session=session,
            config=config,
        )

        # Handle initial `WorkerCreated` events
        self.handle_events(config, workers, scheduler)

        should_reschedule = True

        while not scheduler.is_done():
            if should_reschedule:
                should_reschedule = False

                schedule = self._schedule_with_deadlock_check(workers, scheduler)
                self.submit_work(config, workers, schedule)
            self._wait_for_event()
            should_reschedule |= self.handle_events(config, workers, scheduler)
            should_reschedule |= self._is_all_workers_idle(workers)

        return True

    @pytest.hookimpl
    def pytest_session_handle_event(self, event: Event):
        if event == events.WorkerShutdown:
            self.worker_destroy_queue.append(event.worker_id)

    @pytest.hookimpl
    def pytest_session_handle_metacommand(self, metacommand: SessionMetaCommand):
        # Placeholder, this will be used for `KillWorker`, and `NewWorker` if we decide to add these
        _ = metacommand
        return

    def _add_new_worker(
        self,
        config: pytest.Config,
        workers: dict[WorkerId, Worker],
        session: pytest.Session,
    ):
        worker_id = self.next_worker_id
        self.next_worker_id += 1
        worker = config.hook.pytest_ydist_setup_worker(
            worker_id=worker_id,
            session=session,
            config=config,
            has_events=self.has_events
        )
        workers[worker_id] = worker

    def _destroy_done_workers(self, workers: dict[WorkerId, Worker]):
        while (worker_id := self.worker_destroy_queue.popleft()):
            del workers[worker_id]

    def _schedule_with_deadlock_check(
        self,
        workers: dict[WorkerId, Worker],
        scheduler: Scheduler,
    ) -> Schedule:
        all_idle = self._is_all_workers_idle(workers)
        schedule = scheduler.reschedule()
        if all_idle and len(schedule.new_commands) == 0:
            # raise RuntimeError('Deadlock, due to no items scheduled when all workers idle')
            pass
        return schedule

    def _wait_for_event(self):
        self.has_events.wait()
        self.has_events.clear()

    @staticmethod
    def submit_work(
        config: pytest.Config,
        workers: dict[WorkerId, Worker],
        schedule: Schedule,
    ):
        for metacommand in schedule.new_metacommands:
            if isinstance(metacommand, SessionMetaCommand):
                config.hook.pytest_session_handle_metacommand(metacommand=metacommand)
            elif isinstance(metacommand, WorkerMetaCommand):
                workers[metacommand.worker_id].submit_new_metacommand(metacommand)
            else:
                raise ValueError(f'Unknown metacommand type: {metacommand}')
        for new_command in schedule.new_commands:
            workers[new_command.worker_id].submit_new_command(new_command)

    @staticmethod
    def handle_events(
        config: pytest.Config,
        workers: dict[WorkerId, Worker],
        scheduler: Scheduler,
    ) -> bool:
        """Handle incoming events.

        Some event types are handled inside the session, such as `WorkerDied` or `WorkerFinished`.
        All events are passed to the scheduler.

        Return value indicates whether a rescheduling has been requested.
        """
        reschedule = False

        for worker in workers.values():
            while (event:=worker.pop_event()) is not None:
                config.hook.pytest_session_handle_event(event=event)
                reschedule |= scheduler.notify(event)

        return reschedule


    @staticmethod
    def _is_all_workers_idle(workers):
        return all(worker.is_idle() for worker in workers.values())

