import json
import pickle

import numpy
import pytest

import event_model
from event_model.documents.stream_datum import StreamRange


def test_documents():
    dn = event_model.DocumentNames
    for k in (
        "stop",
        "start",
        "descriptor",
        "event",
        "bulk_events",
        "datum",
        "resource",
        "bulk_datum",
        "event_page",
        "datum_page",
        "stream_resource",
        "stream_datum",
    ):
        assert dn(k) == getattr(dn, k)


def test_len():
    assert 12 == len(event_model.DocumentNames)


def test_schemas():
    for k in event_model.DocumentNames:
        assert k in event_model.SCHEMA_NAMES
        assert event_model.schemas[k]


def test_schema_validators():
    for name in event_model.schemas.keys():
        assert name in event_model.schema_validators

    assert len(event_model.schema_validators) == len(event_model.schemas)


def test_compose_run():
    # Compose each kind of document type. These calls will trigger
    # jsonschema.validate and ensure that the document-generation code composes
    # valid documents.
    bundle = event_model.compose_run()
    start_doc, compose_descriptor, compose_resource, compose_stop = bundle
    assert bundle.start_doc is start_doc
    assert bundle.compose_descriptor is compose_descriptor
    assert bundle.compose_resource is compose_resource
    assert bundle.compose_stop is compose_stop
    bundle = compose_descriptor(
        data_keys={
            "motor": {"shape": [], "dtype": "number", "source": "...", "units": None},
            "image": {
                "shape": [512, 512],
                "dtype": "number",
                "source": "...",
                "external": "FILESTORE:",
            },
        },
        name="primary",
    )
    descriptor_doc, compose_event, compose_event_page = bundle
    assert bundle.descriptor_doc is descriptor_doc
    assert bundle.compose_event is compose_event
    assert bundle.compose_event_page is compose_event_page
    bundle = compose_resource(
        spec="TIFF", root="/tmp", resource_path="stack.tiff", resource_kwargs={}
    )
    resource_doc, compose_datum, compose_datum_page = bundle
    assert bundle.resource_doc is resource_doc
    assert bundle.compose_datum is compose_datum
    assert bundle.compose_datum_page is compose_datum_page
    datum_doc = compose_datum(datum_kwargs={"slice": 5})
    event_doc = compose_event(
        data={"motor": 0, "image": datum_doc["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
    )
    datum_page = compose_datum_page(datum_kwargs={"slice": [10, 15]})
    event_page = compose_event_page(
        data={"motor": [1, 2], "image": datum_page["datum_id"]},
        timestamps={"motor": [0, 0], "image": [0, 0]},
        filled={"image": [False, False]},
        seq_num=[1, 2],
    )
    assert "descriptor" in event_doc
    assert "descriptor" in event_page
    assert event_doc["seq_num"] == 1
    stop_doc = compose_stop()
    assert "primary" in stop_doc["num_events"]
    assert stop_doc["num_events"]["primary"] == 3


def test_compose_stream_resource(tmp_path):
    """
    Following the example of test_compose_run, focus only on the stream resource and
    datum functionality
    """
    bundle = event_model.compose_run()
    compose_stream_resource = bundle.compose_stream_resource
    assert bundle.compose_stream_resource is compose_stream_resource
    bundle = compose_stream_resource(
        mimetype="image/tiff",
        uri="file://localhost" + str(tmp_path) + "/test_streams",
        data_key="det1",
        parameters={},
    )
    resource_doc, compose_stream_datum = bundle
    assert bundle.stream_resource_doc is resource_doc
    assert bundle.compose_stream_datum is compose_stream_datum
    compose_stream_datum(StreamRange(start=0, stop=0), StreamRange(start=0, stop=0))


def test_round_trip_pagination():
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
    res_bundle = run_bundle.compose_resource(
        spec="TIFF", root="/tmp", resource_path="stack.tiff", resource_kwargs={}
    )
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={"slice": 5})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={"slice": 10})
    datum_doc3 = res_bundle.compose_datum(datum_kwargs={"slice": 15})
    event_doc1 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc1["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=1,
    )
    event_doc2 = desc_bundle.compose_event(
        data={"motor": 1, "image": datum_doc2["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=1,
    )
    event_doc3 = desc_bundle.compose_event(
        data={"motor": 2, "image": datum_doc3["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=1,
    )

    # Round trip single event -> event_page -> event.
    expected = event_doc1
    (actual,) = event_model.unpack_event_page(event_model.pack_event_page(expected))
    assert actual == expected

    # Round trip two events -> event_page -> events.
    expected = [event_doc1, event_doc2]
    actual = list(event_model.unpack_event_page(event_model.pack_event_page(*expected)))
    assert actual == expected

    # Round trip three events -> event_page -> events.
    expected = [event_doc1, event_doc2, event_doc3]
    actual = list(event_model.unpack_event_page(event_model.pack_event_page(*expected)))
    assert actual == expected

    # Round trip on docs that don't have a filled key
    unfilled_doc1 = event_doc1
    unfilled_doc1.pop("filled")
    unfilled_doc2 = event_doc2
    unfilled_doc2.pop("filled")
    unfilled_doc3 = event_doc3
    unfilled_doc3.pop("filled")
    expected = [unfilled_doc1, unfilled_doc2, unfilled_doc3]
    actual = list(event_model.unpack_event_page(event_model.pack_event_page(*expected)))
    for doc in actual:
        doc.pop("filled")
    assert actual == expected

    # Round trip one datum -> datum_page -> datum.
    expected = datum_doc1
    (actual,) = event_model.unpack_datum_page(event_model.pack_datum_page(expected))
    assert actual == expected

    # Round trip two datum -> datum_page -> datum.
    expected = [datum_doc1, datum_doc2]
    actual = list(event_model.unpack_datum_page(event_model.pack_datum_page(*expected)))
    assert actual == expected

    # Round trip three datum -> datum_page -> datum.
    expected = [datum_doc1, datum_doc2, datum_doc3]
    actual = list(event_model.unpack_datum_page(event_model.pack_datum_page(*expected)))
    assert actual == expected

    # Check edge case where datum_kwargs are empty.
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={})
    datum_doc3 = res_bundle.compose_datum(datum_kwargs={})

    # Round trip one datum -> datum_page -> datum.
    expected = datum_doc1
    (actual,) = event_model.unpack_datum_page(event_model.pack_datum_page(expected))
    assert actual == expected

    # Round trip two datum -> datum_page -> datum.
    expected = [datum_doc1, datum_doc2]
    actual = list(event_model.unpack_datum_page(event_model.pack_datum_page(*expected)))
    assert actual == expected

    # Round trip three datum -> datum_page -> datum.
    expected = [datum_doc1, datum_doc2, datum_doc3]
    actual = list(event_model.unpack_datum_page(event_model.pack_datum_page(*expected)))
    assert actual == expected


def test_bulk_events_to_event_page(tmp_path):
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

    path_root = str(tmp_path)

    res_bundle = run_bundle.compose_resource(
        spec="TIFF", root=path_root, resource_path="stack.tiff", resource_kwargs={}
    )
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={"slice": 5})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={"slice": 10})
    event1 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc1["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=1,
    )
    event2 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc2["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=2,
    )
    event3 = desc_bundle_baseline.compose_event(
        data={"motor": 0}, timestamps={"motor": 0}, seq_num=1
    )

    primary_event_page = event_model.pack_event_page(event1, event2)
    baseline_event_page = event_model.pack_event_page(event3)
    bulk_events = {"primary": [event1, event2], "baseline": [event3]}
    pages = event_model.bulk_events_to_event_pages(bulk_events)
    assert tuple(pages) == (primary_event_page, baseline_event_page)


def test_sanitize_doc():
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
    event1 = desc_bundle.compose_event(
        data={"motor": 0, "image": numpy.ones((512, 512))},
        timestamps={"motor": 0, "image": 0},
        filled={"image": True},
        seq_num=1,
    )
    event2 = desc_bundle.compose_event(
        data={"motor": 0, "image": numpy.ones((512, 512))},
        timestamps={"motor": 0, "image": 0},
        filled={"image": True},
        seq_num=2,
    )
    event3 = desc_bundle_baseline.compose_event(
        data={"motor": 0}, timestamps={"motor": 0}, seq_num=1
    )

    event_page = event_model.pack_event_page(event1, event2)
    bulk_events = {"primary": [event1, event2], "baseline": [event3]}
    json.dumps(event_model.sanitize_doc(event_page))
    json.dumps(event_model.sanitize_doc(bulk_events))
    json.dumps(event_model.sanitize_doc(event1))


def test_bulk_datum_to_datum_page():
    run_bundle = event_model.compose_run()
    res_bundle = run_bundle.compose_resource(
        spec="TIFF", root="/tmp", resource_path="stack.tiff", resource_kwargs={}
    )
    datum1 = res_bundle.compose_datum(datum_kwargs={"slice": 5})
    datum2 = res_bundle.compose_datum(datum_kwargs={"slice": 10})

    actual = event_model.pack_datum_page(datum1, datum2)
    bulk_datum = {
        "resource": res_bundle.resource_doc["uid"],
        "datum_kwarg_list": [datum1["datum_kwargs"], datum2["datum_kwargs"]],
        "datum_ids": [datum1["datum_id"], datum2["datum_id"]],
    }
    expected = event_model.bulk_datum_to_datum_page(bulk_datum)
    assert actual == expected


def test_document_router_smoke_test():
    dr = event_model.DocumentRouter()
    run_bundle = event_model.compose_run()
    dr("start", run_bundle.start_doc)
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
    dr("descriptor", desc_bundle.descriptor_doc)
    desc_bundle_baseline = run_bundle.compose_descriptor(
        data_keys={"motor": {"shape": [], "dtype": "number", "source": "..."}},
        name="baseline",
    )
    dr("descriptor", desc_bundle_baseline.descriptor_doc)
    res_bundle = run_bundle.compose_resource(
        spec="TIFF", root="/tmp", resource_path="stack.tiff", resource_kwargs={}
    )
    dr("resource", res_bundle.resource_doc)
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={"slice": 5})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={"slice": 10})
    dr("datum", datum_doc1)
    dr("datum", datum_doc2)
    event1 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc1["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=1,
    )
    dr("event", event1)
    event2 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc2["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=2,
    )
    dr("event", event2)
    event3 = desc_bundle_baseline.compose_event(
        data={"motor": 0}, timestamps={"motor": 0}, seq_num=1
    )
    dr("event", event3)
    dr("stop", run_bundle.compose_stop())


def test_document_router_streams_smoke_test(tmp_path):
    dr = event_model.DocumentRouter()
    run_bundle = event_model.compose_run()
    compose_stream_resource = run_bundle.compose_stream_resource
    start = run_bundle.start_doc
    dr("start", start)
    stream_resource_doc, compose_stream_datum = compose_stream_resource(
        mimetype="image/tiff",
        data_key="det1",
        uri="file://localhost" + str(tmp_path) + "/test_streams",
        parameters={},
    )
    dr("stream_resource", stream_resource_doc)
    datum_doc = compose_stream_datum(
        StreamRange(start=0, stop=0), StreamRange(start=0, stop=0)
    )
    dr("stream_datum", datum_doc)
    dr("stop", run_bundle.compose_stop())


def test_document_router_with_validation():
    dr = event_model.DocumentRouter()
    run_bundle = event_model.compose_run()
    dr("start", run_bundle.start_doc, validate=True)
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
    dr("descriptor", desc_bundle.descriptor_doc, validate=True)
    desc_bundle_baseline = run_bundle.compose_descriptor(
        data_keys={"motor": {"shape": [], "dtype": "number", "source": "..."}},
        name="baseline",
    )
    dr("descriptor", desc_bundle_baseline.descriptor_doc, validate=True)
    res_bundle = run_bundle.compose_resource(
        spec="TIFF", root="/tmp", resource_path="stack.tiff", resource_kwargs={}
    )
    dr("resource", res_bundle.resource_doc, validate=True)
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={"slice": 5})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={"slice": 10})
    dr("datum", datum_doc1, validate=True)
    dr("datum", datum_doc2, validate=True)
    event1 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc1["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=1,
    )
    dr("event", event1, validate=True)
    event2 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc2["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=2,
    )
    dr("event", event2, validate=True)
    event3 = desc_bundle_baseline.compose_event(
        data={"motor": 0}, timestamps={"motor": 0}, seq_num=1
    )
    dr("event", event3, validate=True)
    dr("stop", run_bundle.compose_stop(), validate=True)


def test_document_router_dispatch_event():
    event_calls = []  # used for counting calls
    event_page_calls = []  # used for counting calls

    # example documents
    event1 = {
        "data": {"x": 1},
        "timestamps": {"x": 0.0},
        "uid": "placeholder X",
        "descriptor": "placeholder Y",
        "time": 0.0,
        "seq_num": 1,
    }
    event2 = {
        "data": {"x": 2},
        "timestamps": {"x": 1.0},
        "uid": "placeholder X",
        "descriptor": "placeholder Y",
        "time": 1.0,
        "seq_num": 2,
    }
    event_page = event_model.pack_event_page(event1, event2)

    def check(ret, original=None):
        name, doc = ret
        assert doc is not None
        assert doc is not NotImplemented
        if original is not None:
            # Verify that a copy is returned.
            assert doc is not original  # ret is such a poser, dude.
            doc.pop("filled", None)
            original.pop("filled", None)
            assert doc == original

    class DefinesNeitherEventNorEventPage(event_model.DocumentRouter):
        def event(self, doc):
            event_calls.append(object())
            # This returns NotImplemented.
            return super().event_page(doc)

        def event_page(self, doc):
            event_page_calls.append(object())
            # This returns NotImplemented.
            return super().event_page(doc)

    dr = DefinesNeitherEventNorEventPage()
    # Test that Event is routed to Event and EventPage.
    check(dr("event", event1))
    assert len(event_calls) == 1
    assert len(event_page_calls) == 1
    event_calls.clear()
    event_page_calls.clear()
    # Test that EventPage is routed to EventPage and Event *once* before
    # giving up.
    check(dr("event_page", event_page))
    assert len(event_page_calls) == 1
    assert len(event_calls) == 1
    event_calls.clear()
    event_page_calls.clear()

    class DefinesEventNotEventPage(event_model.DocumentRouter):
        def event(self, doc):
            # Just a dumb test that check something particular to these example
            # documents.
            assert doc["data"]["x"] == doc["seq_num"]
            event_calls.append(object())
            return dict(doc)

        def event_page(self, doc):
            event_page_calls.append(object())
            # This returns NotImplemented.
            return super().event_page(doc)

    dr = DefinesEventNotEventPage()
    # Test that Event is routed to Event.
    check(dr("event", event1), event1)
    assert len(event_calls) == 1
    assert len(event_page_calls) == 0
    event_calls.clear()
    event_page_calls.clear()
    # Test that EventPage is unpacked and routed to Event one at a time.
    check(dr("event_page", event_page), event_page)
    assert len(event_page_calls) == 1
    assert len(event_calls) == 2
    event_calls.clear()
    event_page_calls.clear()

    class DefinesEventPageNotEvent(event_model.DocumentRouter):
        def event(self, doc):
            event_calls.append(object())
            # This returns NotImplemented.
            return super().event(doc)

        def event_page(self, doc):
            # Just a dumb test that check something particular to these example
            # documents.
            assert doc["data"]["x"][0] == 1
            event_page_calls.append(object())
            return dict(doc)

    dr = DefinesEventPageNotEvent()
    # Test that Event is packed and routed to EventPage.
    check(dr("event", event1), event1)
    assert len(event_calls) == 1
    assert len(event_page_calls) == 1
    event_calls.clear()
    event_page_calls.clear()
    # Test that EventPage is routed to EventPage.
    check(dr("event_page", event_page), event_page)
    assert len(event_page_calls) == 1
    assert len(event_calls) == 0
    event_calls.clear()
    event_page_calls.clear()

    class DefinesEventPageAndEvent(event_model.DocumentRouter):
        def event(self, doc):
            # Just a dumb test that check something particular to these example
            # documents.
            assert doc["data"]["x"] == doc["seq_num"]
            event_calls.append(object())
            return dict(doc)

        def event_page(self, doc):
            # Just a dumb test that check something particular to these example
            # documents.
            assert doc["data"]["x"][0] == 1
            event_page_calls.append(object())
            return dict(doc)

    dr = DefinesEventPageAndEvent()
    # Test that Event is routed to Event.
    check(dr("event", event1), event1)
    assert len(event_calls) == 1
    assert len(event_page_calls) == 0
    event_calls.clear()
    event_page_calls.clear()
    # Test that EventPage is routed to EventPage.
    check(dr("event_page", event_page), event_page)
    assert len(event_page_calls) == 1
    assert len(event_calls) == 0
    event_calls.clear()
    event_page_calls.clear()


def test_document_router_dispatch_datum():
    datum_calls = []  # used for counting calls
    datum_page_calls = []  # used for counting calls

    # example documents
    datum1 = {
        "datum_id": "placeholder/1",
        "resource": "placeholder",
        "datum_kwargs": {"index": 1},
    }
    datum2 = {
        "datum_id": "placholder/2",
        "resource": "placeholder",
        "datum_kwargs": {"index": 2},
    }
    datum_page = event_model.pack_datum_page(datum1, datum2)

    def check(ret, original=None):
        name, doc = ret
        assert doc is not None
        assert doc is not NotImplemented
        if original is not None:
            # Verify that a copy is returned.
            assert doc is not original  # ret is such a poser, dude.
            assert doc == original

    class DefinesNeitherDatumNorDatumPage(event_model.DocumentRouter):
        def datum(self, doc):
            datum_calls.append(object())
            # This returns NotImplemented.
            return super().datum(doc)

        def datum_page(self, doc):
            datum_page_calls.append(object())
            # This returns NotImplemented.
            return super().datum_page(doc)

    dr = DefinesNeitherDatumNorDatumPage()
    # Test that Datum is routed to Datum and DatumPage.
    check(dr("datum", datum1))
    assert len(datum_calls) == 1
    assert len(datum_page_calls) == 1
    datum_calls.clear()
    datum_page_calls.clear()
    # Test that DatumPage is routed to DatumPage and Datum *once* before giving
    # up.
    check(dr("datum_page", datum_page))
    assert len(datum_page_calls) == 1
    assert len(datum_calls) == 1
    datum_calls.clear()
    datum_page_calls.clear()

    class DefinesDatumNotDatumPage(event_model.DocumentRouter):
        def datum(self, doc):
            # Just a dumb test that check something particular to these example
            # documents.
            assert doc["datum_kwargs"]["index"] == int(doc["datum_id"][-1])
            datum_calls.append(object())
            return dict(doc)

        def datum_page(self, doc):
            datum_page_calls.append(object())
            # This returns NotImplemented.
            return super().datum_page(doc)

    dr = DefinesDatumNotDatumPage()
    # Test that Datum is routed to Datum.
    check(dr("datum", datum1), datum1)
    assert len(datum_calls) == 1
    assert len(datum_page_calls) == 0
    datum_calls.clear()
    datum_page_calls.clear()
    # Test that DatumPage is unpacked and routed to Datum one at a time.
    check(dr("datum_page", datum_page), datum_page)
    assert len(datum_page_calls) == 1
    assert len(datum_calls) == 2
    datum_calls.clear()
    datum_page_calls.clear()

    class DefinesDatumPageNotDatum(event_model.DocumentRouter):
        def datum(self, doc):
            datum_calls.append(object())
            # This returns NotImplemented.
            return super().datum_page(doc)

        def datum_page(self, doc):
            # Just a dumb test that check something particular to these example
            # documents.
            assert doc["datum_kwargs"]["index"][0] == int(doc["datum_id"][0][-1])
            datum_page_calls.append(object())
            return dict(doc)

    dr = DefinesDatumPageNotDatum()
    # Test that Datum is packed and routed to DatumPage.
    check(dr("datum", datum1), datum1)
    assert len(datum_calls) == 1
    assert len(datum_page_calls) == 1
    datum_calls.clear()
    datum_page_calls.clear()
    # Test that DatumPage is routed to DatumPage.
    check(dr("datum_page", datum_page), datum_page)
    assert len(datum_page_calls) == 1
    assert len(datum_calls) == 0
    datum_calls.clear()
    datum_page_calls.clear()
    # Test that DatumPage is routed to DatumPage.

    class DefinesDatumPageAndDatum(event_model.DocumentRouter):
        def datum(self, doc):
            # Just a dumb test that check something particular to these example
            # documents.
            assert doc["datum_kwargs"]["index"] == int(doc["datum_id"][-1])
            datum_calls.append(object())
            return dict(doc)

        def datum_page(self, doc):
            # Just a dumb test that check something particular to these example
            # documents.
            assert doc["datum_kwargs"]["index"][0] == int(doc["datum_id"][0][-1])
            datum_page_calls.append(object())
            return dict(doc)

    dr = DefinesDatumPageAndDatum()
    # Test that Datum is routed to Datum.
    check(dr("datum", datum1), datum1)
    assert len(datum_calls) == 1
    assert len(datum_page_calls) == 0
    datum_calls.clear()
    datum_page_calls.clear()
    # Test that DatumPage is routed to DatumPage.
    check(dr("datum_page", datum_page), datum_page)
    assert len(datum_page_calls) == 1
    assert len(datum_calls) == 0
    datum_calls.clear()
    datum_page_calls.clear()


def test_single_run_document_router():
    sr = event_model.SingleRunDocumentRouter()
    with pytest.raises(event_model.EventModelError):
        sr.get_start()

    run_bundle = event_model.compose_run()
    sr("start", run_bundle.start_doc)
    assert sr.get_start() == run_bundle.start_doc

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
    sr("descriptor", desc_bundle.descriptor_doc)
    desc_bundle_baseline = run_bundle.compose_descriptor(
        data_keys={"motor": {"shape": [], "dtype": "number", "source": "..."}},
        name="baseline",
    )
    sr("descriptor", desc_bundle_baseline.descriptor_doc)
    res_bundle = run_bundle.compose_resource(
        spec="TIFF", root="/tmp", resource_path="stack.tiff", resource_kwargs={}
    )
    sr("resource", res_bundle.resource_doc)
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={"slice": 5})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={"slice": 10})
    sr("datum", datum_doc1)
    sr("datum", datum_doc2)
    event1 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc1["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=1,
    )
    sr("event", event1)
    event2 = desc_bundle.compose_event(
        data={"motor": 0, "image": datum_doc2["datum_id"]},
        timestamps={"motor": 0, "image": 0},
        filled={"image": False},
        seq_num=2,
    )
    sr("event", event2)
    event3 = desc_bundle_baseline.compose_event(
        data={"motor": 0}, timestamps={"motor": 0}, seq_num=1
    )
    sr("event", event3)

    with pytest.raises(event_model.EventModelValueError):
        sr.get_descriptor(res_bundle.resource_doc)

    with pytest.raises(event_model.EventModelValueError):
        sr.get_descriptor(datum_doc1)

    assert sr.get_descriptor(event1) == desc_bundle.descriptor_doc
    assert sr.get_stream_name(event1) == desc_bundle.descriptor_doc.get("name")
    assert sr.get_descriptor(event2) == desc_bundle.descriptor_doc
    assert sr.get_stream_name(event2) == desc_bundle.descriptor_doc.get("name")
    assert sr.get_descriptor(event3) == desc_bundle_baseline.descriptor_doc
    assert sr.get_stream_name(event3) == desc_bundle_baseline.descriptor_doc.get("name")

    desc_bundle_unused = run_bundle.compose_descriptor(
        data_keys={"motor": {"shape": [], "dtype": "number", "source": "..."}},
        name="unused",
    )
    event4 = desc_bundle_unused.compose_event(
        data={"motor": 0}, timestamps={"motor": 0}, seq_num=1
    )

    with pytest.raises(event_model.EventModelValueError):
        sr.get_descriptor(event4)

    with pytest.raises(event_model.EventModelValueError):
        sr.get_stream_name(event4)

    sr("stop", run_bundle.compose_stop())

    # tests against a second run
    run_bundle = event_model.compose_run()
    with pytest.raises(event_model.EventModelValueError):
        sr("start", run_bundle.start_doc)

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
    with pytest.raises(event_model.EventModelValueError):
        sr("descriptor", desc_bundle.descriptor_doc)


@pytest.mark.parametrize("filled", [True, False])
def test_rechunk_event_pages(filled):
    def event_page_gen(page_size, num_pages):
        """
        Generator event_pages for testing.
        """
        data_keys = ["x", "y", "z"]
        array_keys = ["seq_num", "time", "uid"]
        for _ in range(num_pages):
            yield {
                "descriptor": "DESCRIPTOR",
                **{key: list(range(page_size)) for key in array_keys},
                "data": {key: list(range(page_size)) for key in data_keys},
                "timestamps": {key: list(range(page_size)) for key in data_keys},
                "filled": {key: list(range(page_size)) for key in data_keys if filled},
            }

    # Get a list of event pages of size 13.
    event_pages = list(event_page_gen(13, 31))
    # Change the size of the event_pages to size 7.
    event_pages_7 = list(event_model.rechunk_event_pages(event_pages, 7))
    assert [7] * 57 + [4] == [len(page["uid"]) for page in event_pages_7]
    # Change the size back to 13.
    event_pages_13 = event_model.rechunk_event_pages(event_pages_7, 13)
    # Check that it is equal to the original list of event_pages.
    assert event_pages == list(event_pages_13)


def test_rechunk_datum_pages():
    def datum_page_gen(page_size, num_pages):
        """
        Generator datum_pages for testing.
        """
        data_keys = ["x", "y", "z"]
        array_keys = ["datum_id"]
        for _ in range(num_pages):
            yield {
                "resource": "RESOURCE",
                **{key: list(range(page_size)) for key in array_keys},
                "datum_kwargs": {key: list(range(page_size)) for key in data_keys},
            }

    # Get a list of datum pages of size 13.
    datum_pages = list(datum_page_gen(13, 31))
    # Change the size of the datum_pages to size 7.
    datum_pages_7 = list(event_model.rechunk_datum_pages(datum_pages, 7))
    assert [7] * 57 + [4] == [len(page["datum_id"]) for page in datum_pages_7]
    # Change the size back to 13.
    datum_pages_13 = event_model.rechunk_datum_pages(datum_pages_7, 13)
    # Check that it is equal to the original list of datum_pages.
    assert datum_pages == list(datum_pages_13)


def test_pack_empty_raises():
    with pytest.raises(ValueError):
        event_model.pack_event_page()
    with pytest.raises(ValueError):
        event_model.pack_datum_page()


@pytest.mark.parametrize("retry_intervals", [(1,), [1], (), [], None])
def test_retry_intervals_input_normalization(retry_intervals):
    filler = event_model.Filler({}, retry_intervals=retry_intervals, inplace=False)
    assert isinstance(filler.retry_intervals, list)


def test_attempt_with_retires():
    mutable = []
    expected_args = (1, 2)
    expected_kwargs = {"c": 3, "d": 4}
    expected_result = 10

    class LocalException1(Exception):
        pass

    class LocalException2(Exception):
        pass

    def func(*args, **kwargs):
        # Fails when called the first two times;
        # on the third time, returns expected_result.
        assert args == expected_args
        assert kwargs == expected_kwargs
        mutable.append(object())
        if len(mutable) < 3:
            raise LocalException1()
        return expected_result

    # Test with a total of three attempts, just sufficient to succeed.
    result = event_model._attempt_with_retries(
        func=func,
        args=expected_args,
        kwargs=expected_kwargs,
        error_to_catch=LocalException1,
        error_to_raise=LocalException2,
        intervals=[0, 0.01, 0.01],
    )
    assert result == expected_result

    mutable.clear()

    # Test one fewer than the needed number of attempts to succeed.
    with pytest.raises(LocalException2):
        event_model._attempt_with_retries(
            func=func,
            args=expected_args,
            kwargs=expected_kwargs,
            error_to_catch=LocalException1,
            error_to_raise=LocalException2,
            intervals=[0, 0.01],
        )


def test_round_trip_event_page_with_empty_data():
    event_page = {
        "time": [1, 2, 3],
        "seq_num": [1, 2, 3],
        "uid": ["a", "b", "c"],
        "descriptor": "d",
        "data": {},
        "timestamps": {},
        "filled": {},
    }
    events = list(event_model.unpack_event_page(event_page))
    assert len(events) == 3

    page_again = event_model.pack_event_page(*events)
    assert page_again == event_page


def test_round_trip_datum_page_with_empty_data():
    datum_page = {"datum_id": ["a", "b", "c"], "resource": "d", "datum_kwargs": {}}
    datums = list(event_model.unpack_datum_page(datum_page))
    assert len(datums) == 3

    page_again = event_model.pack_datum_page(*datums)
    assert page_again == datum_page


def test_register_coercion():
    # Re-registration should be fine.
    assert "as_is" in event_model._coercion_registry  # implementation detail
    event_model.register_coercion("as_is", event_model.as_is)

    # but registering something different to the same name should raise.
    with pytest.raises(event_model.EventModelValueError):
        event_model.register_coercion("as_is", object)


def test_register_coercion_misspelled():
    "The function register_coercion was originally released as register_coersion."
    # Re-registration should be fine.
    assert "as_is" in event_model._coercion_registry  # implementation detail
    event_model.register_coersion("as_is", event_model.as_is)

    # but registering something different to the same name should raise.
    with pytest.raises(event_model.EventModelValueError):
        event_model.register_coersion("as_is", object)


def test_pickle_filler():
    filler = event_model.Filler({}, inplace=False)
    serialized = pickle.dumps(filler)
    deserialized = pickle.loads(serialized)
    assert filler == deserialized


def test_array_like():
    "Accept any __array__-like as an array."
    dask_array = pytest.importorskip("dask.array")
    bundle = event_model.compose_run()
    desc_bundle = bundle.compose_descriptor(
        data_keys={"a": {"shape": (3,), "dtype": "array", "source": ""}}, name="primary"
    )
    desc_bundle.compose_event_page(
        data={"a": dask_array.ones((5, 3))},
        timestamps={"a": [1, 2, 3, 4, 5]},
        seq_num=[1, 2, 3, 4, 5],
    )


def test_resource_start_optional():
    event_model.compose_resource(
        spec="TEST", root="/", resource_path="", resource_kwargs={}
    )
