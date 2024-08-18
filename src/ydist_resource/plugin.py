from __future__ import annotations

import pytest

from ydist import types as ydist_types

from ydist_resource import hooks
from ydist_resource.scheduler import Scheduler

from ydist_resource.events import (
    pytest_ydist_event_to_serializable,
    pytest_ydist_event_from_serializable,
)
from ydist_resource.commands import (
    pytest_ydist_command_to_serializable,
    pytest_ydist_command_from_serializable,
)
from ydist_resource.worker_plugin import pytest_worker_handle_command

@pytest.hookimpl
def pytest_addhooks(pluginmanager: pytest.PytestPluginManager) -> None:
    pluginmanager.add_hookspecs(hooks)


@pytest.hookimpl
def pytest_ydist_setup_scheduler(session, config) -> ydist_types.Scheduler | None:
    match config.getvalue('ydist_scheduler'):
        case 'resource':
            return Scheduler(session, config)

# Suppress unused warnings, as these hooks just need to be imported to be discovered by pytest
_ = (
    pytest_ydist_event_to_serializable,
    pytest_ydist_event_from_serializable,
    pytest_ydist_command_to_serializable,
    pytest_ydist_command_from_serializable,
    pytest_worker_handle_command,
)
