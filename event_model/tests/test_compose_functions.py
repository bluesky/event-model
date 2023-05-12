from event_model import backwards_compatible_wrapper
from dataclasses import dataclass
from typing import TypedDict


def test_backwards_compatible_wrapper():
    class X(TypedDict):
        u: int
        v: int
        w: int

    @dataclass
    class ComposeX:
        u: int
        v: int = 3

        def __call__(self, w: int):
            return X(u=self.u, v=self.v, w=w)

    compose_x = backwards_compatible_wrapper(ComposeX)
    print(compose_x.__dict__)
    x = compose_x(1, 2, v=3)
    print(x["u"])
    print(x["w"])
    print(x["v"])
    assert x["u"] == 1
    assert x["w"] == 2
    assert x["v"] == 3


test_backwards_compatible_wrapper()
