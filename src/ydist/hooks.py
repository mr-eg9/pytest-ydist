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

@pytest.hookspec(firstresult=True)
def pytest_ydist_event_to_serializable(config: pytest.Config, event: types.Event) -> dict:
    """Convert a ydist event to a serializable type.

    Typically this is a dictionary of simple types.
    """
    ...


@pytest.hookspec(firstresult=True)
def pytest_ydist_event_from_serializable(config: pytest.Config, event_data: dict) -> types.Event:
    """Convert a serializable type representing an event back into an event.

    Note that the `kind` element in the dictionary will contain the name of the type.
    """
    ...


@pytest.hookspec(firstresult=True)
def pytest_ydist_command_to_serializable(config: pytest.Config, command: types.Command) -> dict:
    """Convert a command event to a serializable type.

    Typically this is a dictionary of simple types.
    """
    ...


@pytest.hookspec(firstresult=True)
def pytest_ydist_command_from_serializable(config: pytest.Config, command_data: dict) -> types.Command:
    """Convert a serializable type representing an a command back into a command.

    Note that the `kind` element in the dictionary will contain the name of the type.
    """
    ...


@pytest.hookspec(firstresult=True)
def pytest_ydist_metacommand_to_serializable(config: pytest.Config, metacommand: types.MetaCommand) -> dict:
    """Convert a ydist metacommand to a serializable type.

    Typically this is a dictionary of simple types.
    """
    ...


@pytest.hookspec(firstresult=True)
def pytest_ydist_metacommand_from_serializable(config: pytest.Config, metacommand_data: dict) -> types.MetaCommand:
    """Convert a serializable type representing an metacommand back into a metacommand.

    Note that the `kind` element in the dictionary will contain the name of the type.
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
