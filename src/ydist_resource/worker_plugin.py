from __future__ import annotations

import pytest

from ydist import (
    types as ydist_types,
    commands as ydist_commands,
)

from ydist_resource import commands, events, types

from dataclasses import replace

class ResourceWorkerPlugin:
    def __init__(self, config):
        self.tokens = set()
        self.config = config

    @pytest.hookimpl
    def pytest_worker_handle_command(
        self,
        config: pytest.Config,
        command: ydist_types.Command,
        event_sender: ydist_types.EventSender,
    ):
        match command:
            case commands.RunTestsWithTokens():
                # print(f'Test {command.run_test_command.tests}: {command.tokens}')
                run_test_command = command.run_test_command

                if command.tokens != self.tokens:
                    # Execute the pending test, to ensure all fixtures are torn down
                    # This is a workaround for the fact that we cannot invalidate the
                    #  `ydist_resources` fixture, and so cannot re-create all fixtures downstream
                    #  of `ydist_resources`
                    self._exec_pending_test(config, command.worker_id, event_sender)
                else:
                    # Execute the first test with the resources we currently have
                    self._exec_first_test(config, run_test_command, event_sender)

                # Update the tokens currently in use by this worker
                self._update_tokens(command.tokens, event_sender, command.worker_id)

                # Execute the remaining tests
                config.hook.pytest_worker_handle_command(
                    config=config,
                    command=command.run_test_command,
                    event_sender=event_sender,
                )
            case commands.ReleaseTokens():
                tokens_to_keep = self.tokens.difference(command.tokens)

                # Execute the pending test
                self._exec_pending_test(config, command.worker_id, event_sender)

                self._update_tokens(tokens_to_keep, event_sender, command.worker_id)

    @staticmethod
    def _exec_first_test(
        config: pytest.Config,
        command: ydist_commands.RunTests,
        event_sender: ydist_types.EventSender,
    ):
        first_test_command = replace(command) # Shallow copy
        first_test_command.tests = [command.tests.pop(0)]
        config.hook.pytest_worker_handle_command(
            config=config,
            command=first_test_command,
            event_sender=event_sender,
        )

    @staticmethod
    def _exec_pending_test(config, worker_id, event_sender):
        config.hook.pytest_worker_handle_command(
            config=config,
            command=ydist_commands.RunPendingTest(
                None,
                worker_id,
                ydist_types.CommandStatus.InProgress
            ),
            event_sender=event_sender,
        )

    def _update_tokens(
        self,
        new_tokens: set[types.Token],
        event_sender: ydist_types.EventSender,
        worker_id: ydist_types.WorkerId,
    ):
        tokens_to_release = self.tokens.difference(new_tokens)
        self.tokens = new_tokens
        if len(tokens_to_release) > 0:
            event_sender.send(events.TokensReleased(worker_id, tokens_to_release))

    @pytest.fixture(scope='session')
    def ydist_resources(self) -> set[types.Token]:
        return self.tokens
