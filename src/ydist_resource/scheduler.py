from __future__ import annotations

from collections import deque

import pytest

from ydist import (
    commands as ydist_commands,
    events as ydist_events,
    types as ydist_types,
    utils as ydist_utils,
)

from ydist_resource import types, events, commands

class Scheduler(ydist_types.Scheduler):
    def __init__(self, session: pytest.Session, config: pytest.Config):
        self.session = session
        self.config = config
        self.items = session.items
        self.schedule_tracker = ydist_utils.ScheduleTracker()

        self.remaining_item_idx_by_collection_ids = \
            self._get_item_idx_by_collection_ids(self.config, self.items)

        self.all_tokens = self._get_all_resource_tokens(config)
        self.available_tokens = {item for item in self.all_tokens}
        self.assigned_tokens_by_worker_id: dict[ydist_types.WorkerId, set[types.Token]] = {}
        self._check_all_tests_are_runnable()

    def is_done(self) -> bool:
        if len(self.remaining_item_idx_by_collection_ids) != 0:
            return False
        if len(list(self.schedule_tracker.worker_ids())) != 0:
            return False
        return True

    def notify(self, event: ydist_types.Event) -> bool:
        match event:
            case ydist_events.WorkerStarted():
                self.schedule_tracker.add_worker(event.worker_id)
                self.assigned_tokens_by_worker_id[event.worker_id] = set()
                return True
            case ydist_events.WorkerShutdown():
                del self.assigned_tokens_by_worker_id[event.worker_id]
                remaining_commands = self.schedule_tracker.remove_worker(event.worker_id)
                match remaining_commands:
                    case [ydist_commands.ShutdownWorker()]:
                        pass
                    case []:
                        pass
                    case _:
                        # TODO: Add better error handling
                        raise RuntimeError(f'Worker unexpectedly shut down while there were remaining items: {remaining_commands}')
                return False
            case ydist_events.CommandChangedStatus():
                if event.worker_id not in self.schedule_tracker.worker_ids():
                    # This is a shutdown event
                    return False
                self.schedule_tracker.update_command_status(event.worker_id, event.seq_nr, event.new_status)
                if event.new_status == ydist_types.CommandStatus.Aborted:
                    raise NotImplementedError('Error handling not yet implemented')
                elif event.new_status == ydist_types.CommandStatus.Completed:
                    # TODO: We might need to handle the case where we requested to cancel this command
                    self.schedule_tracker.clear_command(event.worker_id, event.seq_nr)
                    # This is round-robin, so we know there is only 1 command in the queue
                    return True
            case events.TokensReleased():
                self.assigned_tokens_by_worker_id[event.worker_id] -= event.tokens
                self.available_tokens.update(event.tokens)
                return True
            case ydist_events.TestComplete():
                # TODO: Track tests that have been executed, in case a command fails to complete
                # print(f'test {self.items[event.test_idx].name} completed on worker {event.worker_id}')
                pass

        return False


    def reschedule(self) -> ydist_types.Schedule:
        schedule = ydist_types.Schedule()

        unschedulable_workers = set()
        schedulable_workers = set(self.schedule_tracker.worker_ids())
        for i in range(3):
            unschedulable_workers.update(self._schedule_pass(schedule, 0, schedulable_workers))
            schedulable_workers -= unschedulable_workers

        all_idle = len(unschedulable_workers) == len(list(self.schedule_tracker.worker_ids()))
        if all_idle and len(self.remaining_item_idx_by_collection_ids) == 0:
            self._schedule_worker_shutdowns(schedule)

        return schedule

    def _schedule_pass(self, schedule: ydist_types.Schedule, depth: int, worker_ids: set[ydist_types.WorkerId]):
        valid_worker_ids_at_depth = set()
        unscheduable_workers = set()
        scheduled_workers = set()
        for worker_id in worker_ids:
            worker_commands = self.schedule_tracker.get_commands(worker_id)
            if len(worker_commands) != depth:
                continue
            valid_worker_ids_at_depth.add(worker_id)

        # If a resource set _can_ be assigned to a worker at this depth, then do so
        for worker_id in valid_worker_ids_at_depth:
            runnable = self._find_runnable_collection_ids(
                {*self.available_tokens, *self.assigned_tokens_by_worker_id[worker_id]}
            )
            if runnable is None:
                unscheduable_workers.add(worker_id)
            else:
                collection_ids, tokens = runnable
                scheduled_workers.add(worker_id)
                self._schedule_tests_with_resources(schedule, worker_id, collection_ids, tokens)
        valid_worker_ids_at_depth -= scheduled_workers

        # If at depth 0, and we cannot assign any more tests to the existing workers, then
        #  we should ask them to release their current resources if they have any, this is required
        #  to avoid deadlocks
        if depth == 0:
            for worker_id in valid_worker_ids_at_depth:
                tokens = {token for token in self.assigned_tokens_by_worker_id[worker_id]}
                if len(tokens) == 0:
                    continue
                self.schedule_tracker.schedule_command(
                    schedule,
                    worker_id,
                    commands.ReleaseTokens,
                    tokens
                )

        return unscheduable_workers

    def _schedule_worker_shutdowns(self, schedule: ydist_types.Schedule):
        for worker_id in self.schedule_tracker.worker_ids():
            worker_commands = self.schedule_tracker.get_commands(worker_id)
            if len(worker_commands) == 0:
                self.schedule_tracker.schedule_command(schedule, worker_id, ydist_commands.ShutdownWorker)

    def _find_runnable_collection_ids(self, available_tokens):
        for collection_ids in self.remaining_item_idx_by_collection_ids.keys():
            tokens = self._get_token_set_to_use_for_collection_ids(
                collection_ids,
                available_tokens,
            )
            if tokens is not None:
                return collection_ids, tokens

    def _check_all_tests_are_runnable(self):
        unrunnable_collection_ids = []
        for collection_ids in self.remaining_item_idx_by_collection_ids:
            tokens = self._get_token_set_to_use_for_collection_ids(
                collection_ids,
                self.all_tokens,
            )
            if tokens is None:
                unrunnable_collection_ids.append(collection_ids)

        for collection_ids in unrunnable_collection_ids:
            test_idx = self.remaining_item_idx_by_collection_ids[collection_ids]
            match self.config.getvalue('on_missing_resource'):
                case 'crash':
                    raise RuntimeError(f'Failed to resolve resource requirements for test {test_idx[0]}')
                case 'fail':
                    self._fake_run_all_tests_with_collection_ids(collection_ids, self.run_pytest_fail)
                case 'skip':
                    self._fake_run_all_tests_with_collection_ids(collection_ids, self.run_pytest_skip)

    def _fake_run_all_tests_with_collection_ids(self, collection_ids, run_fn):
        call_info = pytest.CallInfo.from_call(run_fn, 'setup')
        item_idxs = self.remaining_item_idx_by_collection_ids.pop(collection_ids)
        for item_idx in item_idxs:
            report = self.config.hook.pytest_runtest_makereport(item=self.items[item_idx], call=call_info)
            self.config.hook.pytest_runtest_logreport(report=report)

    @staticmethod
    def run_pytest_skip():
        pytest.skip("Skipped due to missing resources (--on-missing-resources=skip)")

    @staticmethod
    def run_pytest_fail():
        pytest.fail("Failed due to missing resources (--on-missing-resources=fail)")

    def _schedule_tests_with_resources(
        self,
        schedule: ydist_types.Schedule,
        worker_id: ydist_types.WorkerId,
        collection_ids: list[types.CollectionId],
        tokens: set[types.Token]
    ):
        self.available_tokens -= tokens
        self.assigned_tokens_by_worker_id[worker_id].update(tokens)
        tests = self.remaining_item_idx_by_collection_ids.pop(collection_ids)
        self.schedule_tracker.schedule_command(
            schedule,
            worker_id,
            commands.RunTestsWithTokens,
            run_test_command=ydist_commands.RunTests(
                None, # This is an abstract/fake type, so the SeqNr should not matter
                worker_id,
                ydist_types.CommandStatus.Pending,
                tests,
            ),
            tokens=tokens,
        )

    def _get_token_set_to_use_for_collection_ids(self, collection_id, tokens) -> set[types.Token] | None:
        first_test_idx = self.remaining_item_idx_by_collection_ids[collection_id][0]
        first_test = self.items[first_test_idx]
        token_sets = self.config.hook.pytest_ydist_resource_tokens_from_test_item(
            item=first_test,
            tokens=tokens,
        )

        if any(token_set is types.ResourcesNotAvailable for token_set in token_sets):
            return None

        token_set_to_use = set()
        for token_set in token_sets:
            token_set_to_use.update(token_set)
        return token_set_to_use


    @staticmethod
    def _get_item_idx_by_collection_ids(config: pytest.Config, items: list[pytest.Item]):
        test_items_by_group = {}
        for i, item in enumerate(items):
            collection_ids = tuple(
                config.hook.pytest_ydist_resource_collection_id_from_test_item(item=item)
            )
            assert all(isinstance(collection_id, types.CollectionId) for collection_id in collection_ids), \
                f'Expected all elements to be `CollectionId`, received: {collection_ids}'
            test_items_by_group.setdefault(collection_ids, []).append(i)
        return test_items_by_group

    @staticmethod
    def _get_all_resource_tokens(config: pytest.Config) -> set[types.Token]:
        all_tokens = set()
        tokens_list: list[set[types.Token]] = config.hook.pytest_ydist_resource_get_tokens()
        assert all(isinstance(tokens, set) for tokens in tokens_list), \
            f'Expected to receive a list[set[`ResourceToken`]], received {tokens_list}'
        for tokens in tokens_list:
            assert all(isinstance(token, types.Token) for token in tokens), \
                f'Expected to receive a list[set[`ResourceToken`]], received {tokens_list}'
            all_tokens.update(tokens)
        return all_tokens

