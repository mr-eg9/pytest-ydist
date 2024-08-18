from __future__ import annotations

from dataclasses import dataclass, asdict

import pytest

from ydist import (
    events as ydist_events,
    types as ydist_types,
)

from ydist_resource import types

@dataclass
class TokensReleased(ydist_events.Event):
    tokens: set[types.Token]


@pytest.hookspec(firstresult=True)
def pytest_ydist_event_to_serializable(config: pytest.Config, event: ydist_types.Event) -> dict | None:
    """Convert a ydist event to a serializable type.

    Typically this is a dictionary of simple types.
    """
    _ = config
    match event:
        case TokensReleased():
            event_data = asdict(event)
            event_data['kind'] = TokensReleased.__name__
            return event_data


@pytest.hookspec(firstresult=True)
def pytest_ydist_event_from_serializable(config: pytest.Config, event_data: dict) -> ydist_types.Event | None:
    """Convert a serializable type representing an event back into an event.

    Note that the `kind` element in the dictionary will contain the name of the type.
    """
    _ = config
    match event_data['kind']:
        case TokensReleased.__name__:
            event_data.pop('kind')
            return TokensReleased(**event_data)
