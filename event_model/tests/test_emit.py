import gc

import pytest

import event_model


def test_emit():
    collector = []

    def cb(name, doc):
        collector.append((name, doc))

    router = event_model.DocumentRouter(emit=cb)
    name = "start"
    doc = {"uid": "asdf", "time": 0}
    router.emit(name, doc)
    assert len(collector) == 1
    assert collector == [(name, doc)]

    # Test that we hold a weakref only.
    del cb
    gc.collect()
    router.emit(name, doc)
    assert len(collector) == 1


def test_emit_with_method():
    collector = []

    class Thing:
        def cb(self, name, doc):
            collector.append((name, doc))

    thing = Thing()

    router = event_model.DocumentRouter(emit=thing.cb)
    name = "start"
    doc = {"uid": "asdf", "time": 0}
    router.emit(name, doc)
    assert len(collector) == 1
    assert collector == [(name, doc)]

    # Test that we hold a weakref only.
    del thing
    gc.collect()
    router.emit(name, doc)
    assert len(collector) == 1


def test_emit_validation():
    """
    Anything but a callable that accepts two arguments should raise ValueError.
    """

    # Not callable
    with pytest.raises(ValueError):
        event_model.DocumentRouter(emit=1)

    # Wrong callback signature
    with pytest.raises(ValueError):
        event_model.DocumentRouter(emit=lambda: None)

    # Wrong callback signature
    with pytest.raises(ValueError):
        event_model.DocumentRouter(emit=lambda a: None)

    # Wrong callback signature
    with pytest.raises(ValueError):
        event_model.DocumentRouter(emit=lambda a, b, c: None)

    # Right callback signature
    event_model.DocumentRouter(emit=lambda a, b: None)
