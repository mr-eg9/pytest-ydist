from __future__ import annotations


import pytest
from ydist_resource import types as yr_types


def test_foo(request):
    print(f'foo is running on worker: {request.config.getvalue('ydist_worker_id')}')
    assert False

def test_bar(request):
    print(f'bar is running on worker: {request.config.getvalue('ydist_worker_id')}')
    assert False

def test_baz(request):
    print(f'baz is running on worker: {request.config.getvalue('ydist_worker_id')}')
    assert False
