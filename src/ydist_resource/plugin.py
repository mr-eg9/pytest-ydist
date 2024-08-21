from __future__ import annotations

import pytest

from ydist import types as ydist_types

from ydist_resource import (
    hooks,
    worker_plugin,
    types,
)
from ydist_resource.scheduler import Scheduler

from ydist_resource.events import (
    pytest_ydist_register_events,
)
from ydist_resource.commands import (
    pytest_ydist_register_commands,
)
from ydist_resource.types import (
    pytest_ydist_resource_register_tokens,
)

@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    if config.getvalue('ydist_worker_addr'):
        session = worker_plugin.ResourceWorkerPlugin(config)
        config.pluginmanager.register(session, 'pytest_ydist_resource_worker_plugin')

@pytest.hookimpl
def pytest_addhooks(pluginmanager: pytest.PytestPluginManager) -> None:
    pluginmanager.add_hookspecs(hooks)


@pytest.hookimpl
def pytest_sessionstart(session: pytest.Session):
    types._token_types = {
        token_cls.__qualname__: token_cls
        for registered_token_types in session.config.hook.pytest_ydist_resource_register_tokens()
        for token_cls in registered_token_types
    }


@pytest.hookimpl
def pytest_ydist_setup_scheduler(session, config) -> ydist_types.Scheduler | None:
    match config.getvalue('ydist_scheduler'):
        case 'resource':
            return Scheduler(session, config)


# Suppress unused warnings, as these hooks just need to be imported to be discovered by pytest
_ = (
    pytest_ydist_register_events,
    pytest_ydist_register_commands,
    pytest_ydist_resource_register_tokens,
)
