from __future__ import annotations

import pytest
from ydist_resource import types as yr_types


foo_token = yr_types.Token('fool')
bar_token = yr_types.Token('barista')
baz_token = yr_types.Token('bazed')


@pytest.hookimpl()
def pytest_ydist_resource_get_tokens() -> set[yr_types.Token] | None:
    return {
        foo_token,
        bar_token,
        baz_token,
    }


@pytest.hookimpl()
def pytest_ydist_resource_collection_id_from_test_item(item: pytest.Item) -> yr_types.CollectionId | None:
    if 'foo' in item.name:
        return yr_types.CollectionId(1)
    if 'bar' in item.name:
        return yr_types.CollectionId(2)
    if 'baz' in item.name:
        return yr_types.CollectionId(3)


@pytest.hookimpl()
def pytest_ydist_resource_tokens_from_test_item(
    item: pytest.Item,
    tokens: set[yr_types.Token],
) -> set[yr_types.Token] | None:
    if 'foo' in item.name and foo_token in tokens:
        return {foo_token}
    if 'bar' in item.name and bar_token in tokens:
        return {bar_token}
    if 'baz' in item.name and baz_token in tokens:
        return {baz_token}
    return set()


