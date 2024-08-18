from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from typing import Hashable, Any

import pytest

from ydist import types as ydist_types, commands as ydist_commands, events as ydist_events
from ydist.utils import ScheduleTracker


@dataclass
class Token:
    token: Hashable

    def __hash__(self):
        return hash((self.__class__, self.token))


@dataclass
class CollectionId:
    collection_id: Hashable

    def __hash__(self):
        return hash((self.__class__, self.collection_id))


@dataclass
class RunTestsWithTokens(ydist_types.Command):
    run_test_command: ydist_commands.RunTests
    tokens: list[Token]


@dataclass
class TokensReleased(ydist_events.Event):
    tokens: set[Token]


@pytest.hookspec()
def pytest_ydist_resource_get_tokens() -> set[Token] | None:
    """Determine the available resources on the system, and generate unique tokens for them.

    This is used on both the worker and the client, to track which resources exist on the system.

    NOTE: Its important to ensure that the resource tokens are unique, so for plugin development,
    or inside a project, we recommend creating a sub-type of `ResourceToken`.
    """
    ...


@pytest.hookspec()
def pytest_ydist_resource_collection_id_from_test_item(item: pytest.Item) -> CollectionId | None:
    """Detemine the associated `collection_id` for a specified test item.

    This mechanism is used to group together tests that share the same resource requirements,
    which allows the scheduler to run a lot more efficiently.

    NOTE: Its important to ensure that the collection ID is unique, so for plugin development,
    or inside a project, we recommend creating a dataclass sub-type of `ResourceCollectionId`,
    e.g.: `class MyResourceCollectionId(ResourceCollectionId)`

    NOTE: This function will be called for every test that returned by collection some degree
        of memoization is recommended for optimal performance if possible.

    :return:
        `MyResourceCollectionId(None)` or anthoer sentinel value if the test does not require any resources
        `MyResourceCollectionId(collection_id)` if you can enumerate this collection id
    """
    ...


@pytest.hookspec()
def pytest_ydist_resource_tokens_from_test_item(
    item: pytest.Item,
    tokens: set[Token],
) -> set[Token] | None:
    """Determine a set of `ResourceToken` items that would fullfill the requirements of `test_item`.

    NOTE: `tokens` **must not** be modified by this function.
    NOTE: This function may be called repeatedly for the same `test_item` so some degree of
        memoization is recommended for optimal performance.
    """
    ...


@pytest.hookspec(firstresult=True)
def pytest_ydist_event_to_serializable(config: pytest.Config, event: ydist_types.Event) -> dict | None:
    """Convert a ydist event to a serializable type.

    Typically this is a dictionary of simple types.
    """
    match event:
        case TokensReleased():
            event_data = asdict(event)
            event_data['kind'] = TokensReleased.__name__
            return event_data


@pytest.hookspec(firstresult=True)
def pytest_ydist_event_from_serializable(config: pytest.Config, event_data: dict) -> ydist_types.Event | None:
    """Convert a serializable type representing an event back into an event.

    Note that the `kind` element in the dictionary will contain the name of the type.
    """
    match event_data['kind']:
        case TokensReleased.__name__:
            event_data.pop('kind')
            return TokensReleased(**event_data)


@pytest.hookspec(firstresult=True)
def pytest_ydist_command_to_serializable(config: pytest.Config, command: ydist_types.Command) -> dict | None:
    """Convert a command event to a serializable type.

    Typically this is a dictionary of simple types.
    """
    match command:
        case RunTestsWithTokens:
            command_data = asdict(command)
            command_data['run_test_command'] = config.hook.pytest_ydist_command_to_serializable(
                config=config,
                command=command_data['run_test_command'])
            command_data['status'] = command_data['status'].name
            command_data['kind'] = command.__class__.__name__
            return command_data



@pytest.hookspec(firstresult=True)
def pytest_ydist_command_from_serializable(config: pytest.Config, command_data: dict) -> ydist_types.Command | None:
    """Convert a serializable type representing an a command back into a command.

    Note that the `kind` element in the dictionary will contain the name of the type.
    """
    match command_data['kind']:
        case RunTestsWithTokens.__name__:
            command_data.pop('kind')
            command_data['status'] = ydist_types.CommandStatus[command_data['status']]
            command_data['run_test_command'] = config.hook.pytest_ydist_command_from_serializable(
                config=config,
                command_data=command_data['run_test_command']
            )
            return RunTestsWithTokens(**command_data)


class Scheduler(ydist_types.Scheduler):
    def __init__(self, session: pytest.Session, config: pytest.Config):
        self.session = session
        self.config = config
        self.items = session.items
        self.schedule_tracker = ScheduleTracker()

        self.remaining_item_idx_by_collection_ids = \
            self._get_item_idx_by_collection_ids(self.config, self.items)

        self.all_tokens = self._get_all_resource_tokens(config)
        self.available_tokens = {item for item in self.all_tokens}
        self.assigned_tokens_by_worker_id: dict[ydist_types.WorkerId, set[Token]] = {}
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
                return True
            case ydist_events.WorkerShutdown():
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
            case TokensReleased():
                self.assigned_tokens_by_worker_id[event.worker_id] -= event.tokens
                self.available_tokens.update(event.tokens)
                return True
            case ydist_events.TestComplete():
                # TODO: Track tests that have been executed, in case a command fails to complete
                pass

        return False


    def reschedule(self) -> ydist_types.Schedule:
        # TODO: Optimize the scheduler a bit further
        schedule = ydist_types.Schedule()

        all_idle = True

        for worker_id in self.schedule_tracker.worker_ids():
            command_count = len(self.schedule_tracker.get_commands(worker_id))
            if command_count > 0:
                all_idle = False

            if command_count < 3:
                for collection_ids, test_idx in self.remaining_item_idx_by_collection_ids.items():
                    tokens = self._get_token_set_to_use_for_collection_ids(
                        self.config,
                        self.items,
                        test_idx,
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
                self.config,
                self.items,
                self.remaining_item_idx_by_collection_ids[collection_ids],
                self.all_tokens,
            )
            assert tokens is not None, f'Failed to resolve resource requirements for test {test_idx[0]}'

    def _schedule_tests_with_resources(
        self,
        schedule: ydist_types.Schedule,
        worker_id: ydist_types.WorkerId,
        collection_ids: list[CollectionId],
        tokens: set[Token]
    ):
        self.available_tokens -= tokens
        self.assigned_tokens_by_worker_id[worker_id].update(tokens)
        tests = self.remaining_item_idx_by_collection_ids.pop(collection_ids)
        self.schedule_tracker.schedule_command(
            schedule,
            worker_id,
            RunTestsWithTokens,
            run_test_command=ydist_commands.RunTests(
                None, # This is an abstract/fake type, so the SeqNr should not matter
                worker_id,
                ydist_types.CommandStatus.Pending,
                tests,
            ),
            tokens=tokens,
        )

    @staticmethod
    def _get_token_set_to_use_for_collection_ids(config, items, item_idx, tokens) -> set[Token] | None:
        first_test = items[item_idx[0]]
        token_sets = config.hook.pytest_ydist_resource_tokens_from_test_item(
            first_test=first_test, tokens=tokens)

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
            collection_ids = config.hook.pytest_dist_resource_collection_id_from_test_item(
                item=item)
            assert all(isinstance(collection_id, CollectionId) for collection_id in collection_ids), \
                f'Expected all elements to be `CollectionId`, received: {collection_ids}'
            test_items_by_group.setdefault(collection_ids, deque()).append(i)
        return test_items_by_group

    @staticmethod
    def _get_all_resource_tokens(config: pytest.Config) -> set[Token]:
        all_tokens = set()
        tokens_list: list[set[Token]] = config.hook.pytest_ydist_resource_get_tokens()
        assert all(isinstance(tokens, set) for tokens in tokens_list), \
            f'Expected to receive a list[set[`ResourceToken`]], received {tokens_list}'
        for tokens in tokens_list:
            assert all(isinstance(token, Token) for token in tokens), \
                f'Expected to receive a list[set[`ResourceToken`]], received {tokens_list}'
            all_tokens.update(tokens)
        return all_tokens

