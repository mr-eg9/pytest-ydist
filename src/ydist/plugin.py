import pytest

from ydist.session import Session
from ydist.workers.psworker import WorkerProccess
from ydist.workers.psworker import ProccessWorker
from ydist.schedulers.round_robin import RoundRobinScheduler
from ydist import hooks
from ydist import types
from ydist import events
from ydist import commands

pytest_plugins = ("ydist")

@pytest.hookimpl
def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("ydist", "distributed testing")
    group._addoption(
        "--numworkers",
        "-n",
        dest="numworkers",
        type=str,
        action="store",
        help="Defines how many workers should be created for parallel test execution"
    )
    group._addoption(
        "--ydist-worker-addr",
        dest="ydist_worker_addr",
        type=str,
        action="store",
        help="Used to specify the ydist worker address when initializing worker subproccesses"
    )
    group._addoption(
        "--ydist-worker-id",
        dest="ydist_worker_id",
        type=int,
        action="store",
        help="Tells the ydist worker proccess its id"
    )

@pytest.hookimpl
def pytest_addhooks(pluginmanager: pytest.PytestPluginManager) -> None:
    pluginmanager.add_hookspecs(hooks)

@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    # return
    if config.getvalue('ydist_worker_addr'):
        session = WorkerProccess(config)
        config.pluginmanager.register(session, 'pytest_worker_session')
        config.pluginmanager.register(events, 'ydist_events')
        config.pluginmanager.register(commands, 'ydist_commands')
    else:
        session = Session(config)
        config.pluginmanager.register(session, 'ydist_main_session')
        config.pluginmanager.register(events, 'ydist_events')
        config.pluginmanager.register(commands, 'ydist_commands')


@pytest.hookimpl(trylast=True)
def pytest_ydist_setup_worker(worker_id, session, config, has_events) -> types.Worker:
    return ProccessWorker(worker_id, session, config, has_events)


@pytest.hookimpl(trylast=True)
def pytest_ydist_setup_scheduler(session, config) -> types.Scheduler:
    return RoundRobinScheduler(session, config)
