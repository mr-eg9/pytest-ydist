import pytest

from ydist.session import Session

pytest_plugins = ("ydist")

@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    session = Session(config)
    config.pluginmanager.register(session, 'dsession')
