import pytest
import os

from ydist.session import Session
from ydist.workers.psworker import WorkerProccess
from ydist.workers.psworker import ProccessWorker
from ydist.schedulers.round_robin import RoundRobinScheduler
from ydist import hooks
from ydist import types

pytest_plugins = ("ydist")

# Let pytest discover the hooks we have implemented
from ydist.events import (
    pytest_ydist_register_events,
)
from ydist.commands import (
    pytest_ydist_register_commands,
)
from ydist.metacommands import (
    pytest_ydist_register_metacommands,
)

@pytest.hookimpl
def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup('ydist', 'distributed testing')
    group._addoption(
        '--ydist-numworkers',
        dest='ydist_numworkers',
        type=int,
        default=0,
        action='store',
        help='Defines how many workers should be created for parallel test execution. If 0, then ydist is disabled.'
    )
    group._addoption(
        '--ydist-worker',
        dest='ydist_worker',
        type=str,
        default='proccess',
        action='store',
        help='Used to specify the worker type to be used by ydist.',
    )
    group._addoption(
        '--ydist-scheduler',
        dest='ydist_scheduler',
        type=str,
        default='dist',
        action='store',
        help='Used to specify the scheduler type to be used by ydist.'
    )
    group._addoption(
        '--ydist-worker-addr',
        dest='ydist_worker_addr',
        type=str,
        action='store',
        help='Used to specify the ydist worker address when initializing worker subproccesses'
    )
    group._addoption(
        '--ydist-worker-id',
        dest='ydist_worker_id',
        type=int,
        action='store',
        help='Tells the ydist worker proccess its id'
    )


@pytest.hookimpl
def pytest_addhooks(pluginmanager: pytest.PytestPluginManager) -> None:
    pluginmanager.add_hookspecs(hooks)


@pytest.hookimpl(wrapper=True)
def pytest_cmdline_main(config):
    # We need to do this patching before `pytest_configure` runs for any plugins,
    #  as many plugins change behavior in `pytest_configure` if they are in a xdist-worker
    worker_id: str = config.getvalue('ydist_worker_id')  # type: ignore
    if worker_id is not None:
        config.workerinput = {}  # type: ignore
        os.environ['PYTEST_XDIST_WORKER'] = str(worker_id)

    yield


@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config):
    if config.getvalue('ydist_worker_addr'):
        session = WorkerProccess(config)
        config.pluginmanager.register(session, 'pytest_ydist_worker_session')
    else:
        enabled = config.getvalue('ydist_numworkers') > 0  # type: ignore
        session = Session(config, enabled)
        config.pluginmanager.register(session, 'pytest_ydist_main_session')


@pytest.hookimpl(trylast=True)
def pytest_ydist_setup_worker(worker_id, session, config, has_events) -> types.Worker | None:
    match config.getvalue('ydist_worker'):
        case 'proccess':
            return ProccessWorker(worker_id, session, config, has_events)


@pytest.hookimpl(trylast=True)
def pytest_ydist_setup_scheduler(session, config) -> types.Scheduler | None:
    match config.getvalue('ydist_scheduler'):
        case 'dist':
            return RoundRobinScheduler(session, config)


# Suppress unused warnings, as these hooks just need to be imported to be discovered by pytest
_ = (
    pytest_ydist_register_events,
    pytest_ydist_register_commands,
    pytest_ydist_register_metacommands,
)
