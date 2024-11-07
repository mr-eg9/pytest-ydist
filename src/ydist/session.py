from __future__ import annotations

import pytest
from ydist.metacommands import SessionMetaCommand, WorkerMetaCommand
from ydist.types import WorkerId, Worker, Schedule, Scheduler, Event

from ydist import events
from collections import deque

from threading import Event as MpEvent

class Session:
    """The `Session` instance used by this plugin"""
    def __init__(self, config: pytest.Config, enabled: bool):

        self.numworkers: int = config.getoption('ydist_numworkers', 'auto')  # type: ignore
        if self.numworkers == 0 and enabled:
            raise ValueError('--ydist-numworkers cannot be 0 while ydist is still enabled')

        self.has_events = MpEvent()
        self.next_worker_id = WorkerId(0)
        self.worker_destroy_queue = deque()
        self.ready_workers = set()
        self.enabled = enabled

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtestloop(self, session: pytest.Session):
        """Main testloop.

        This procedure forms the backbone of the concurrency model here.
        """
        if not self.enabled:
            return None
        if session.testsfailed and not session.config.option.continue_on_collection_errors:
            raise session.Interrupted(
                "%d error%s during collection"
                % (session.testsfailed, "s" if session.testsfailed != 1 else "")
            )

        if session.config.option.collectonly:
            return True

        config: pytest.Config = session.config
        workers: dict[WorkerId, Worker] = {}

        try:
            for _ in range(self.numworkers):
                self._add_new_worker(config, workers, session)

            scheduler = self._setup_scheduler(config, session)

            # Handle initial `WorkerCreated` events
            while len(self.ready_workers) < self.numworkers:
                self.handle_events(config, workers, scheduler)

            self._pytest_ydist_main_loop(config, workers, scheduler)
        except Exception as e:
            for worker in workers.values():
                worker.terminate()
            raise e

        return True

    def _pytest_ydist_main_loop(self, config, workers, scheduler):
        should_reschedule = True

        while not scheduler.is_done():
            if should_reschedule:
                should_reschedule = False

                schedule = self._schedule_with_deadlock_check(workers, scheduler)
                self.submit_work(config, workers, schedule)
            self._wait_for_event()
            should_reschedule |= self.handle_events(config, workers, scheduler)
            should_reschedule |= self._is_all_workers_idle(workers)

    @pytest.hookimpl
    def pytest_session_handle_event(self, config: pytest.Config, event: Event):
        match event:
            case events.WorkerStarted():
                self.ready_workers.add(event.worker_id)
            case events.WorkerShutdown():
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
        if worker is None:
            raise ValueError('No ydist worker specifed (--ydist-worker)')
        workers[worker_id] = worker

    def _setup_scheduler(self, config, session) -> Scheduler:
        scheduler = config.hook.pytest_ydist_setup_scheduler(
            session=session,
            config=config,
        )
        if scheduler is None:
            raise ValueError(f'No ydist scheduler specified (--ydist-scheduler)')
        return scheduler

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
            raise RuntimeError('Deadlock, due to no items scheduled when all workers idle')
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
                config.hook.pytest_session_handle_metacommand(
                    config=config,
                    metacommand=metacommand,
                )
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
                config.hook.pytest_session_handle_event(config=config, event=event)
                reschedule |= scheduler.notify(event)

        return reschedule


    @staticmethod
    def _is_all_workers_idle(workers):
        return all(worker.is_idle() for worker in workers.values())

