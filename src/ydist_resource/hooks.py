from __future__ import annotations

import pytest

from ydist_resource import types

@pytest.hookspec()
def pytest_ydist_resource_get_tokens() -> set[types.Token] | None:
    """Determine the available resources on the system, and generate unique tokens for them.

    This is used on both the worker and the client, to track which resources exist on the system.

    NOTE: Its important to ensure that the resource tokens are unique, so for plugin development,
    or inside a project, we recommend creating a sub-type of `ResourceToken`.
    """
    ...


@pytest.hookspec()
def pytest_ydist_resource_collection_id_from_test_item(item: pytest.Item) -> types.CollectionId | None:
    """Detemine the associated `collection_id` for a specified test item.

    This mechanism is used to group together tests that share the same resource requirements,
    which allows the scheduler to run a lot more efficiently.

    NOTE: Its important to ensure that the collection ID is unique, so for plugin development,
    or inside a project, we recommend creating a dataclass sub-type of `ResourceCollectionId`,
    e.g.: `class MyResourceCollectionId(ResourceCollectionId)`

    NOTE: This function will be called for every test that returned by collection some degree
        of memoization is recommended for optimal performance if possible.

    :return:
        `MyResourceCollectionId(None)` or anthoer sentinel value if the test does not require any resources
        `MyResourceCollectionId(collection_id)` if you can enumerate this collection id
    """
    ...


@pytest.hookspec()
def pytest_ydist_resource_tokens_from_test_item(
    item: pytest.Item,
    tokens: set[types.Token],
) -> set[types.Token] | None:
    """Determine a set of `ResourceToken` items that would fullfill the requirements of `test_item`.

    NOTE: `tokens` **must not** be modified by this function.
    NOTE: This function may be called repeatedly for the same `test_item` so some degree of
        memoization is recommended for optimal performance.
    """
    ...


@pytest.hookspec
def pytest_ydist_resource_register_tokens() -> list[type[types.Token]]:
    """Register custom token types for use with ydist resource.

    This is used by ydist resource to be able to deserialize tokens.
    """
    ...
