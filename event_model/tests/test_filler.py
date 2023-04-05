import copy
import pathlib

import numpy
import pytest

import event_model

path_root = pathlib.Path("/placeholder/path")
run_bundle = event_model.compose_run()
desc_bundle = run_bundle.compose_descriptor(
    data_keys={
        "motor": {"shape": [], "dtype": "number", "source": "..."},
        "image": {
            "shape": [512, 512],
            "dtype": "number",
            "source": "...",
            "external": "FILESTORE:",
        },
    },
    name="primary",
)
desc_bundle_baseline = run_bundle.compose_descriptor(
    data_keys={"motor": {"shape": [], "dtype": "number", "source": "..."}},
    name="baseline",
)
res_bundle = run_bundle.compose_resource(
    spec="DUMMY",
    root=str(path_root),
    resource_path="stack.tiff",
    resource_kwargs={"a": 1, "b": 2},
)
datum_doc = res_bundle.compose_datum(datum_kwargs={"c": 3, "d": 4})
raw_event = desc_bundle.compose_event(
    data={"motor": 0, "image": datum_doc["datum_id"]},
    timestamps={"motor": 0, "image": 0},
    filled={"image": False},
    seq_num=1,
)
stop_doc = run_bundle.compose_stop()


class DummyHandler:
    def __init__(self, resource_path, a, b):
        assert a == 1
        assert b == 2
        assert resource_path == str(path_root / "stack.tiff")

    def __call__(self, c, d):
        assert c == 3
        assert d == 4
        return numpy.ones((5, 5))


reg = {"DUMMY": DummyHandler}


@pytest.fixture
def filler():
    filler = event_model.Filler(reg, inplace=True)
    filler("start", run_bundle.start_doc)
    filler("descriptor", desc_bundle.descriptor_doc)
    filler("descriptor", desc_bundle_baseline.descriptor_doc)
    filler("resource", res_bundle.resource_doc)
    filler("datum", datum_doc)
    event = copy.deepcopy(raw_event)
    assert isinstance(event["data"]["image"], str)
    filler("event", event)
    assert event["data"]["image"].shape == (5, 5)
    filler("stop", stop_doc)
    assert not filler.closed
    return filler


def test_no_filler():
    "Test that NoFiller does not fill data."
    filler = event_model.NoFiller(reg)
    filler("start", run_bundle.start_doc)
    filler("descriptor", desc_bundle.descriptor_doc)
    filler("descriptor", desc_bundle_baseline.descriptor_doc)
    filler("resource", res_bundle.resource_doc)
    filler("datum", datum_doc)
    event = copy.deepcopy(raw_event)
    assert isinstance(event["data"]["image"], str)
    filler("event", event)
    # Check that it *hasn't* been filled.
    assert isinstance(event["data"]["image"], str)
    filler("stop", stop_doc)


def test_get_handler(filler):
    "Test the method get_handler() which should always return a fresh instance."
    handler = filler.get_handler(res_bundle.resource_doc)
    # The method does not expose the internal cache of handlers, so it should
    # not return the same instance when called repeatedly.
    assert filler.get_handler(res_bundle.resource_doc) is not handler


def test_close(filler):
    "Closing a Filler should make it impossible to add documents."
    filler.close()
    with pytest.raises(event_model.EventModelRuntimeError):
        filler.get_handler(res_bundle.resource_doc)
    with pytest.raises(event_model.EventModelRuntimeError):
        filler("stop", stop_doc)


def test_context_manager():
    "Construct Filler as a context manager."
    with event_model.Filler(reg, inplace=True) as filler:
        filler("start", run_bundle.start_doc)
        filler("descriptor", desc_bundle.descriptor_doc)
        filler("descriptor", desc_bundle_baseline.descriptor_doc)
        filler("resource", res_bundle.resource_doc)
        filler("datum", datum_doc)
        event = copy.deepcopy(raw_event)
        name, doc = filler("event", event)
        assert name == "event"
        assert doc is event
        filler("stop", stop_doc)
        assert not filler.closed
    assert event["data"]["image"].shape == (5, 5)
    assert filler.closed


def test_context_manager_with_event_page():
    with event_model.Filler(reg, inplace=True) as filler:
        filler("start", run_bundle.start_doc)
        filler("descriptor", desc_bundle.descriptor_doc)
        filler("descriptor", desc_bundle_baseline.descriptor_doc)
        filler("resource", res_bundle.resource_doc)
        filler("datum", datum_doc)
        event_page = event_model.pack_event_page(copy.deepcopy(raw_event))
        name, doc = filler("event_page", event_page)
        assert name == "event_page"
        assert doc is event_page
        filler("stop", stop_doc)
        assert not filler.closed
    assert event_page["data"]["image"][0].shape == (5, 5)
    assert filler.closed


def test_undefined_handler_spec():
    "Check failure path when an unknown spec is found in a resource."
    with event_model.Filler({}, inplace=True) as filler:
        filler("start", run_bundle.start_doc)
        filler("descriptor", desc_bundle.descriptor_doc)
        filler("descriptor", desc_bundle_baseline.descriptor_doc)
        filler("resource", res_bundle.resource_doc)
        filler("datum", datum_doc)
        event = copy.deepcopy(raw_event)
        assert isinstance(event["data"]["image"], str)
        with pytest.raises(event_model.UndefinedAssetSpecification):
            filler("event", event)


def test_include_and_exclude():
    with pytest.raises(ValueError):
        event_model.Filler({}, include=[], exclude=[], inplace=True)

    with pytest.warns(DeprecationWarning):
        with event_model.Filler(reg, exclude=["image"], inplace=True) as filler:
            filler("start", run_bundle.start_doc)
            filler("descriptor", desc_bundle.descriptor_doc)
            filler("descriptor", desc_bundle_baseline.descriptor_doc)
            filler("resource", res_bundle.resource_doc)
            filler("datum", datum_doc)
            event = copy.deepcopy(raw_event)
            assert isinstance(event["data"]["image"], str)
            filler("event", event)
            filler("stop", stop_doc)

    with pytest.warns(DeprecationWarning):
        with event_model.Filler(reg, include=["image"], inplace=True) as filler:
            filler("start", run_bundle.start_doc)
            filler("descriptor", desc_bundle.descriptor_doc)
            filler("descriptor", desc_bundle_baseline.descriptor_doc)
            filler("resource", res_bundle.resource_doc)
            filler("datum", datum_doc)
            event = copy.deepcopy(raw_event)
            filler("event", event)
            filler("stop", stop_doc)
            assert not filler.closed
    assert event["data"]["image"].shape == (5, 5)

    with pytest.warns(DeprecationWarning):
        with event_model.Filler(
            reg, include=["image", "EXTRA THING"], inplace=True
        ) as filler:
            filler("start", run_bundle.start_doc)
            filler("descriptor", desc_bundle.descriptor_doc)
            filler("descriptor", desc_bundle_baseline.descriptor_doc)
            filler("resource", res_bundle.resource_doc)
            filler("datum", datum_doc)
            event = copy.deepcopy(raw_event)
            filler("event", event)
            filler("stop", stop_doc)
            assert not filler.closed
    assert event["data"]["image"].shape == (5, 5)


def test_root_map():
    new_path = pathlib.Path("/another_placeholder", "moved")

    class DummyHandlerRootMapTest:
        def __init__(self, resource_path, a, b):
            assert a == 1
            assert b == 2
            assert resource_path == str(new_path / "stack.tiff")

        def __call__(self, c, d):
            assert c == 3
            assert d == 4
            return numpy.ones((5, 5))

    with event_model.Filler(
        {"DUMMY": DummyHandlerRootMapTest},
        root_map={str(path_root): str(new_path)},
        inplace=True,
    ) as filler:
        filler("start", run_bundle.start_doc)
        filler("descriptor", desc_bundle.descriptor_doc)
        filler("descriptor", desc_bundle_baseline.descriptor_doc)
        filler("resource", res_bundle.resource_doc)
        filler("datum", datum_doc)
        event = copy.deepcopy(raw_event)
        filler("event", event)
        filler("stop", stop_doc)
        assert not filler.closed
    assert event["data"]["image"].shape == (5, 5)


def test_verify_filled(filler):
    "Test the utility function verify_filled."
    with pytest.raises(event_model.UnfilledData):
        event_model.verify_filled(event_model.pack_event_page(raw_event))
    event = copy.deepcopy(raw_event)
    name, doc = filler("event", event)
    event_model.verify_filled(event_model.pack_event_page(event))


def test_inplace():
    "Test the behavior of the 'inplace' parameter."
    with event_model.Filler(reg, inplace=True) as filler:
        filler("start", run_bundle.start_doc)
        filler("descriptor", desc_bundle.descriptor_doc)
        filler("descriptor", desc_bundle_baseline.descriptor_doc)
        filler("resource", res_bundle.resource_doc)
        filler("datum", datum_doc)
        # Test event()
        event = copy.deepcopy(raw_event)
        name, filled_event = filler("event", event)
        assert filled_event is event
        event = copy.deepcopy(raw_event)
        # Test fill_event()
        filled_event = filler.fill_event(event)
        assert filled_event is event
        # Test event_page()
        event_page = event_model.pack_event_page(copy.deepcopy(raw_event))
        _, filled_event_page = filler("event_page", event_page)
        assert filled_event_page is event_page
        # Test fill_event_page()
        event_page = event_model.pack_event_page(copy.deepcopy(raw_event))
        filled_event_page = filler.fill_event_page(event_page)
        assert filled_event_page is event_page

        # Test fill_event and fill_event_page again with inplace=False.

        # Test fill_event()
        filled_event = filler.fill_event(event, inplace=False)
        assert filled_event is not event
        # Test fill_event_page()
        event_page = event_model.pack_event_page(copy.deepcopy(raw_event))
        filled_event_page = filler.fill_event_page(event_page, inplace=False)
        assert filled_event_page is not event_page

    with event_model.Filler(reg, inplace=False) as filler:
        filler("start", run_bundle.start_doc)
        filler("descriptor", desc_bundle.descriptor_doc)
        filler("descriptor", desc_bundle_baseline.descriptor_doc)
        filler("resource", res_bundle.resource_doc)
        filler("datum", datum_doc)
        event = copy.deepcopy(raw_event)
        name, filled_event = filler("event", event)
        assert filled_event is not event
        assert isinstance(event["data"]["image"], str)

        event = copy.deepcopy(raw_event)
        # Test fill_event()
        filled_event = filler.fill_event(event)
        assert filled_event is not event
        # Test event_page()
        event_page = event_model.pack_event_page(copy.deepcopy(raw_event))
        _, filled_event_page = filler("event_page", event_page)
        assert filled_event_page is not event_page
        # Test fill_event_page()
        event_page = event_model.pack_event_page(copy.deepcopy(raw_event))
        filled_event_page = filler.fill_event_page(event_page)
        assert filled_event_page is not event_page

        # Test fill_event and fill_event_page again with inplace=True.

        # Test fill_event()
        filled_event = filler.fill_event(event, inplace=True)
        assert filled_event is event
        # Test fill_event_page()
        event_page = event_model.pack_event_page(copy.deepcopy(raw_event))
        filled_event_page = filler.fill_event_page(event_page, inplace=True)
        assert filled_event_page is event_page

    with pytest.warns(UserWarning):
        # warnings because inplace is not specified
        filler = event_model.Filler(reg)


def test_handler_registry_access():
    "Test the handler_registery can be viewed but is immutable."

    class OtherDummyHandler:
        "Same as DummyHandler, but a different object to test mutating reg"

        def __init__(self, resource_path, a, b):
            assert a == 1
            assert b == 2
            assert resource_path == str(path_root / "stack.tiff")

        def __call__(self, c, d):
            assert c == 3
            assert d == 4
            return numpy.ones((5, 5))

    with event_model.Filler(reg, inplace=False) as filler:
        with pytest.raises(event_model.EventModelTypeError):
            # Updating an existing key fails.
            filler.handler_registry["DUMMY"] = OtherDummyHandler
        with pytest.raises(event_model.EventModelTypeError):
            # Setting a new key fails.
            filler.handler_registry["SOMETHING_ELSE"] = OtherDummyHandler
        with pytest.raises(event_model.EventModelTypeError):
            # Deleting a item fails.
            del filler.handler_registry["DUMMY"]
        filler("start", run_bundle.start_doc)
        filler("descriptor", desc_bundle.descriptor_doc)
        filler("descriptor", desc_bundle_baseline.descriptor_doc)
        filler("resource", res_bundle.resource_doc)
        filler("datum", datum_doc)
        event = copy.deepcopy(raw_event)
        name, filled_event = filler("event", event)
        assert filled_event is not event
        assert isinstance(event["data"]["image"], str)
        # Now there should be a handler instance in the cache.
        assert filler._handler_cache  # implementation detail
        with pytest.raises(event_model.DuplicateHandler):
            filler.register_handler("DUMMY", OtherDummyHandler)
        filler.register_handler("DUMMY", OtherDummyHandler, overwrite=True)
        assert filler.handler_registry["DUMMY"] is OtherDummyHandler
        # Replacing the handler for a given spec should clear the cache.
        assert not filler._handler_cache  # implementation detail
        # Filling should work the same....
        filler("start", run_bundle.start_doc)
        filler("descriptor", desc_bundle.descriptor_doc)
        filler("descriptor", desc_bundle_baseline.descriptor_doc)
        filler("resource", res_bundle.resource_doc)
        filler("datum", datum_doc)
        event = copy.deepcopy(raw_event)
        name, filled_event = filler("event", event)
        assert filled_event is not event
        assert isinstance(event["data"]["image"], str)
        handler = filler.deregister_handler("DUMMY")
        assert handler is OtherDummyHandler
        assert not filler.handler_registry
        assert not filler._handler_cache  # implementation detail


def test_mismatched_data_keys():
    "Test that we raise specifically when data keys do not match"
    "between event and descriptor."
    with pytest.raises(event_model.MismatchedDataKeys):
        with event_model.NoFiller(reg) as filler:
            filler("start", run_bundle.start_doc)
            filler("descriptor", desc_bundle.descriptor_doc)
            filler("descriptor", desc_bundle_baseline.descriptor_doc)
            filler("resource", res_bundle.resource_doc)
            filler("datum", datum_doc)
            event = copy.deepcopy(raw_event)
            del event["data"]["image"]
            filler("event", event)

    with pytest.raises(event_model.MismatchedDataKeys):
        with event_model.Filler(reg, inplace=False) as filler:
            filler("start", run_bundle.start_doc)
            filler("descriptor", desc_bundle.descriptor_doc)
            filler("descriptor", desc_bundle_baseline.descriptor_doc)
            filler("resource", res_bundle.resource_doc)
            filler("datum", datum_doc)
            event = copy.deepcopy(raw_event)
            del event["data"]["image"]
            filler("event", event)


def test_clear_caches(filler):
    "Test that methods that clear the caches work."
    assert filler._descriptor_cache  # implementation detail
    assert filler._resource_cache  # implementation detail
    filler.clear_document_caches()
    assert not filler._descriptor_cache  # implementation detail
    assert not filler._resource_cache  # implementation detail

    assert filler._handler_cache  # implementation detail
    filler.clear_handler_cache()
    assert not filler._handler_cache  # implementation detail
