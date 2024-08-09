import pytest

@pytest.fixture(scope='session')
def foo(request):
    pass

@pytest.mark.parametrize('foo', ['Patrick!', 'Spongebob!'], indirect=True)
@pytest.mark.parametrize('bar,baz', [('hi', 'whats'), ('there', 'up')])
def test_foo(foo, bar, baz):
    # assert bar == 'hi'
    pass

# def test_bar():
#     pass
