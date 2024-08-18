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
        self._ydist_worker_plugin = None

    @pytest.hookimpl
    def pytest_worker_handle_command(
        self,
        config: pytest.Config,
        command: ydist_types.Command,
        event_sender: ydist_types.EventSender,
    ):
        match command:
            case commands.RunTestsWithTokens():
                tokens = command.tokens
                run_test_command = command.run_test_command

                # Execute the first test with the resources we currently have
                self._exec_first_test(config, run_test_command, event_sender)

                # Update the tokens currently in use by this worker
                self._update_tokens(tokens, event_sender, command.worker_id)

                # Execute the remaining tests
                config.hook.pytest_worker_handle_command(
                    config=config,
                    command=command.run_test_command,
                    event_sender=event_sender,
                )

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

    @pytest.fixture
    def ydist_resources(self) -> set[types.Token]:
        return self.tokens
