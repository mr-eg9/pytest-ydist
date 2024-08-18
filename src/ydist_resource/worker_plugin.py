from __future__ import annotations

import pytest

from ydist import types as ydist_types

from ydist_resource import commands


@pytest.hookimpl
def pytest_worker_handle_command(config: pytest.Config, command: ydist_types.Command):
    match command:
        case commands.RunTestsWithTokens():
            _ = command.tokens # TODO: Implement relevant logic
            config.hook.pytest_worker_handle_command(config=config, command=command.run_test_command)
