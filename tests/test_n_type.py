import pickle


def test_0990():
    """Test the N container class"""
    n = N(a='b', c=3)
    assert n['a'] == 'b'
    assert n.a == 'b'
    assert n.c == 3
    assert list(n.items()) == [('a', 'b'), ('c', 3)]

    n.a = 'xyz'
    assert n['a'] == 'xyz'
    assert n.a == 'xyz'
    assert n.c == 3
    assert list(n.items()) == [('a', 'xyz'), ('c', 3)]

    n2 = N(c=4, d='abc')
    assert n2 is not n
    assert n2 == {'c': 4, 'd': 'abc'}

    n3 = N(n)
    assert n3 is n
    assert list(n3.items()) == [('a', 'xyz'), ('c', 3)]


def test_1000():
    """Pickle and unpickle"""
    n = N(a='b', c=3)
    p1 = pickle.dumps(n)
    n2 = pickle.loads(p1)
    assert n == n2
