import pytest

from ydist.session import Session
from ydist.workers.psworker import WorkerProccess

pytest_plugins = ("ydist")

@pytest.hookimpl
def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("ydist", "distributed testing")
    group._addoption(
        "--ydist-worker-addr",
        dest="ydist_worker_addr",
        action="store",
        help="Used to specify the ydist worker address when initializing worker subproccesses"
    )

@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    # return
    if config.getvalue('ydist_worker_addr'):
        session = WorkerProccess(config)
        config.pluginmanager.register(session, 'ydist_worker_session')
    else:
        session = Session(config)
        config.pluginmanager.register(session, 'ydist_session')
