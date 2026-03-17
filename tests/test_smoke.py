def test_smoke():
    assert 1 + 1 == 2


def test_rust_binding_hello():
    from pytest_rxdist.core import hello

    assert hello("world") == "hello, world"
