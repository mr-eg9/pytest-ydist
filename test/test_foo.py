import pytest

@pytest.fixture(scope='session')
def foo(request):
    pass

@pytest.mark.parametrize('huh', list(range(100_000)))
def test_foo(huh):
    # assert bar == 'hi'
    pass

# def test_bar():
#     pass
