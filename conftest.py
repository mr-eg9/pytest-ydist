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
    id_nu = 0
    if 'foo' in item.name:
        id_nu |= 1
    if 'bar' in item.name:
        id_nu |= 2
    if 'baz' in item.name:
        id_nu |= 4
    return yr_types.CollectionId(id_nu)


@pytest.hookimpl()
def pytest_ydist_resource_tokens_from_test_item(
    item: pytest.Item,
    tokens: set[yr_types.Token],
) -> set[yr_types.Token] | type[yr_types.ResourcesNotAvailable]:
    selected_tokens = set()
    if 'foo' in item.name:
        if foo_token not in tokens:
            return yr_types.ResourcesNotAvailable
        selected_tokens.add(foo_token)

    if 'bar' in item.name:
        if bar_token not in tokens:
            return yr_types.ResourcesNotAvailable
        selected_tokens.add(bar_token)

    if 'baz' in item.name:
        if baz_token not in tokens:
            return yr_types.ResourcesNotAvailable
        selected_tokens.add(baz_token)

    return selected_tokens
