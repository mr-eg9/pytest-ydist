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
                    case [ydist_commands.ShutdownWorker]:
                        pass
                    case []:
                        pass
                    case _:
                        # TODO: Add better error handling
                        raise RuntimeError('Worker unexpectedly shut down while there were remaining items')
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
                pass

        return False


    def reschedule(self) -> ydist_types.Schedule:
        # TODO: Change scheduling to:
        #  1. Ensure all workers have 1 item
        #  2. Ensure all workers have 2 items
        #  3. <...>
        #  Also remember to optimize, so that workers can be reassigned tasks they can run
        #   with the items allready allocated (if any)
        schedule = ydist_types.Schedule()

        all_idle = True

        for worker_id in self.schedule_tracker.worker_ids():
            command_count = len(self.schedule_tracker.get_commands(worker_id))
            if command_count > 0:
                all_idle = False

            for collection_ids in list(self.remaining_item_idx_by_collection_ids.keys()):
                if command_count < 1:
                    tokens = self._get_token_set_to_use_for_collection_ids(
                        collection_ids,
                        self.available_tokens,
                    )
                    if tokens is not None:
                        self._schedule_tests_with_resources(schedule, worker_id, collection_ids, tokens)
                        command_count += 1

        if all_idle and len(self.remaining_item_idx_by_collection_ids) == 0:
            for worker_id in self.schedule_tracker.worker_ids():
                self.schedule_tracker.schedule_command(schedule, worker_id, ydist_commands.ShutdownWorker)

        return schedule

    def _check_all_tests_are_runnable(self):
        for collection_ids in self.remaining_item_idx_by_collection_ids:
            test_idx = self.remaining_item_idx_by_collection_ids[collection_ids]
            tokens = self._get_token_set_to_use_for_collection_ids(
                collection_ids,
                self.all_tokens,
            )
            assert tokens is not None, f'Failed to resolve resource requirements for test {test_idx[0]}'

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

        if any(token_set is None for token_set in token_sets):
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

