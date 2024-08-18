import pytest

class HarnessA:
    def __init__(self, param1, param2):
        pass


class HarnessB:
    def __init__(self):
        pass


@pytest.hookimpl
def pytest_ydist_resource_get_tokens():
    # Figure out what resources are globally available, called once per session
    pass

@pytest.hookimpl
def pyetst_ydist_get_descr(item):
    # Test item -> descr via e.g. marks
    # Supports MultiResult in order to support plugins/composability
    pass

@pytest.hookimpl
def pytest_ydist_resource_tokens_from_descr(descr, tokens: list):
    # descr, tokens -> set(Token) | None
    # If a set of tokens is returned, then this is assumed to be a success,
    #  if None is returned then that means the resource is either `Unknown` or `NotAvailable`
    pass


@pytest.mark.parametrize('harness_a', [HarnessA(0, 0), HarnessA(0, 1)])
def test_foo(harness_a: HarnessA):
    pass

@pytest.mark.parametrize('harness_b', [HarnessA(0, 0)])
def test_bar(harness_b):
    pass

@pytest.mark.parametrize('harness_a,harness_b', [(HarnessA(0, 0), HarnessB())])
def test_baz(harness_a, harness_b):
    pass
