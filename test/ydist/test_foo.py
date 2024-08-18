import pytest

@pytest.fixture(scope='session')
def foo(request):
    pass

@pytest.mark.parametrize('huh', list(range(30)))
# @pytest.mark.parametrize('huh', list(range(100_000)))
def test_foo(huh):
    # assert bar == 'hi'
    # if huh % 100 == 0:
    # assert False, 'We dont like the 100s'
    if huh == 23:
        assert False, 'We dont like 23'
    pass

# def test_bar():
#     pass
