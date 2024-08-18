from __future__ import annotations

from dataclasses import dataclass, asdict

import pytest

from ydist import (
    commands as ydist_commands,
    types as ydist_types,
)

from ydist_resource import types

@dataclass
class RunTestsWithTokens(ydist_types.Command):
    run_test_command: ydist_commands.RunTests
    tokens: set[types.Token]


@pytest.hookimpl
def pytest_ydist_command_to_serializable(config: pytest.Config, command: ydist_types.Command) -> dict | None:
    """Convert a command event to a serializable type.

    Typically this is a dictionary of simple types.
    """
    match command:
        case RunTestsWithTokens():
            tokens = [
                config.hook.pytest_ydist_resource_token_to_serializable(config=config, token=token)
                for token in command.tokens
            ]
            command_data = asdict(command)
            command_data['run_test_command'] = config.hook.pytest_ydist_command_to_serializable(
                config=config,
                command=command.run_test_command
            )
            command_data['tokens'] = tokens
            command_data['status'] = command_data['status'].name
            command_data['kind'] = command.__class__.__name__
            return command_data


@pytest.hookimpl
def pytest_ydist_command_from_serializable(config: pytest.Config, command_data: dict) -> ydist_types.Command | None:
    """Convert a serializable type representing an a command back into a command.

    Note that the `kind` element in the dictionary will contain the name of the type.
    """
    match command_data['kind']:
        case RunTestsWithTokens.__name__:
            command_data.pop('kind')
            command_data['run_test_command'] = config.hook.pytest_ydist_command_from_serializable(
                config=config,
                command_data=command_data['run_test_command']
            )
            command_data['tokens'] = {
                config.hook.pytest_ydist_resource_token_from_serializable(
                    config=config, token_data=token_data)
                for token_data in command_data['tokens']
            }
            command_data['status'] = ydist_types.CommandStatus[command_data['status']]
            return RunTestsWithTokens(**command_data)
