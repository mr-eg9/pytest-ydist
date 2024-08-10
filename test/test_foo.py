import pytest

@pytest.fixture(scope='session')
def foo(request):
    pass

@pytest.mark.parametrize('foo', ['Patrick!', 'Spongebob!'], indirect=True)
@pytest.mark.parametrize('bar,baz', [('hi', 'whats'), ('there', 'up')])
@pytest.mark.parametrize('huh', list(range(10)))
def test_foo(foo, bar, baz, huh):
    # assert bar == 'hi'
    pass

# def test_bar():
#     pass
