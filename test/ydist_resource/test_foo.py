from __future__ import annotations


import pytest
from ydist_resource import types as yr_types


foo_token = yr_types.Token('fool')
bar_token = yr_types.Token('barista')
baz_token = yr_types.Token('bazed')


def test_foo(ydist_resources):
    assert ydist_resources == {foo_token}

def test_bar(ydist_resources):
    assert ydist_resources == {bar_token}

def test_baz(ydist_resources):
    assert ydist_resources == {baz_token}

def test_foo_bar_baz(ydist_resources):
    assert ydist_resources == {foo_token, bar_token, baz_token}
