import pytest

from ydist import types, metacommands

# ------------------------------------------------------------------------------
# Used to define your own workers/schedulers
# ------------------------------------------------------------------------------

@pytest.hookspec(firstresult=True)
def pytest_ydist_setup_worker(worker_id, session, config, has_events) -> types.Worker:
    """Set up a ydist `Worker`.

    This is meant to allow downstream plugins to define their own workers.
    """
    ...


@pytest.hookspec(firstresult=True)
def pytest_ydist_setup_scheduler(session, config) -> types.Scheduler:
    """Set up a ydist `Scheduler`.

    This is meant to allow downstream plugins to define their own scheduler.
    """
    ...

# ------------------------------------------------------------------------------
# Used to deal with serialization / deserialization of custom Command/Event types
# ------------------------------------------------------------------------------


@pytest.hookspec
def pytest_ydist_register_events() -> list[type[types.Event]]:
    """Register custom event types for use with ydist.

    This is used by ydist to be able to deserialize events.
    """
    ...


@pytest.hookspec
def pytest_ydist_register_commands() -> list[type[types.Command]]:
    """Register command types for use with ydist.

    This is used by ydist to be able to deserialize commands.
    """
    ...


@pytest.hookspec
def pytest_ydist_register_metacommands() -> list[type[types.MetaCommand]]:
    """Register metacommand types for use with ydist.

    This is used by ydist to be able to deserialize metacommands.
    """
    ...


# ------------------------------------------------------------------------------
# Used to handle custom events, commands or metacommands
# ------------------------------------------------------------------------------


@pytest.hookspec()
def pytest_session_handle_event(
    config: pytest.Config,
    event: types.Event
):
    """Handle an event in the session.

    This function will be called for all generated events.
    This is meant to allow downstream plugins to embed their own logic on events.
    """
    ...


@pytest.hookspec()
def pytest_session_handle_metacommand(
    config: pytest.Config,
    metacommand: metacommands.SessionMetaCommand
):
    """Handle a metacommand in the session.

    This function be called to execute a metacommand in the session.
    This is meant to allow downstream plugins to embed handling of custom defined commands,
    or modify the handling of ydist commands.
    """
    ...


@pytest.hookspec()
def pytest_worker_handle_command(
    config: pytest.Config,
    command: types.Command,
    event_sender: types.EventSender
):
    """Handle a command on the worker.

    This function be called to execute a command in the worker.
    This is meant to allow downstream plugins to embed handling of custom defined commands,
    or modify the handling of ydist commands.
    """
    ...


@pytest.hookspec()
def pytest_worker_handle_metacommand(
    config: pytest.Config,
    metacommand: metacommands.WorkerMetaCommand,
    event_sender: types.EventSender,
):
    """Handle a metacommand on the worker.

    This function will be called to execute a metacommand in the worker.
    This is meant to allow downstream plugins to embed handling of custom defined metacommands,
    or modify the handling of ydist metacommands.
    """
    ...
