import copy
import json
import event_model
import numpy
import pytest


def test_documents():
    dn = event_model.DocumentNames
    for k in ('stop', 'start', 'descriptor',
              'event', 'bulk_events', 'datum',
              'resource', 'bulk_datum', 'event_page', 'datum_page'):
        assert dn(k) == getattr(dn, k)


def test_len():
    assert 10 == len(event_model.DocumentNames)


def test_schemas():
    for k in event_model.DocumentNames:
        assert k in event_model.SCHEMA_NAMES
        assert event_model.schemas[k]


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
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'},
                   'image': {'shape': [512, 512], 'dtype': 'number',
                             'source': '...', 'external': 'FILESTORE:'}},
        name='primary')
    descriptor_doc, compose_event, compose_event_page = bundle
    assert bundle.descriptor_doc is descriptor_doc
    assert bundle.compose_event is compose_event
    assert bundle.compose_event_page is compose_event_page
    bundle = compose_resource(
        spec='TIFF', root='/tmp', resource_path='stack.tiff',
        resource_kwargs={})
    resource_doc, compose_datum, compose_datum_page = bundle
    assert bundle.resource_doc is resource_doc
    assert bundle.compose_datum is compose_datum
    assert bundle.compose_datum_page is compose_datum_page
    datum_doc = compose_datum(datum_kwargs={'slice': 5})
    event_doc = compose_event(
        data={'motor': 0, 'image': datum_doc['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False})
    assert 'descriptor' in event_doc
    assert event_doc['seq_num'] == 1
    stop_doc = compose_stop()
    assert 'primary' in stop_doc['num_events']


def test_round_trip_pagination():
    run_bundle = event_model.compose_run()
    desc_bundle = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'},
                   'image': {'shape': [512, 512], 'dtype': 'number',
                             'source': '...', 'external': 'FILESTORE:'}},
        name='primary')
    res_bundle = run_bundle.compose_resource(
        spec='TIFF', root='/tmp', resource_path='stack.tiff',
        resource_kwargs={})
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={'slice': 5})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={'slice': 10})
    datum_doc3 = res_bundle.compose_datum(datum_kwargs={'slice': 15})
    event_doc1 = desc_bundle.compose_event(
        data={'motor': 0, 'image': datum_doc1['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False},
        seq_num=1)
    event_doc2 = desc_bundle.compose_event(
        data={'motor': 1, 'image': datum_doc2['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False},
        seq_num=1)
    event_doc3 = desc_bundle.compose_event(
        data={'motor': 2, 'image': datum_doc3['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False},
        seq_num=1)

    # Round trip single event -> event_page -> event.
    expected = event_doc1
    actual, = event_model.unpack_event_page(
        event_model.pack_event_page(expected))
    assert actual == expected

    # Round trip two events -> event_page -> events.
    expected = [event_doc1, event_doc2]
    actual = list(event_model.unpack_event_page(
        event_model.pack_event_page(*expected)))
    assert actual == expected

    # Round trip three events -> event_page -> events.
    expected = [event_doc1, event_doc2, event_doc3]
    actual = list(event_model.unpack_event_page(
        event_model.pack_event_page(*expected)))
    assert actual == expected

    # Round trip on docs that don't have a filled key
    unfilled_doc1 = event_doc1
    unfilled_doc1.pop('filled')
    unfilled_doc2 = event_doc2
    unfilled_doc2.pop('filled')
    unfilled_doc3 = event_doc3
    unfilled_doc3.pop('filled')
    expected = [unfilled_doc1, unfilled_doc2, unfilled_doc3]
    actual = list(event_model.unpack_event_page(
        event_model.pack_event_page(*expected)))
    for doc in actual:
        doc.pop('filled')
    assert actual == expected

    # Round trip one datum -> datum_page -> datum.
    expected = datum_doc1
    actual, = event_model.unpack_datum_page(
        event_model.pack_datum_page(expected))
    assert actual == expected

    # Round trip two datum -> datum_page -> datum.
    expected = [datum_doc1, datum_doc2]
    actual = list(event_model.unpack_datum_page(
        event_model.pack_datum_page(*expected)))
    assert actual == expected

    # Round trip three datum -> datum_page -> datum.
    expected = [datum_doc1, datum_doc2, datum_doc3]
    actual = list(event_model.unpack_datum_page(
        event_model.pack_datum_page(*expected)))
    assert actual == expected


def test_bulk_events_to_event_page(tmp_path):
    run_bundle = event_model.compose_run()
    desc_bundle = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'},
                   'image': {'shape': [512, 512], 'dtype': 'number',
                             'source': '...', 'external': 'FILESTORE:'}},
        name='primary')
    desc_bundle_baseline = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'}},
        name='baseline')

    path_root = str(tmp_path)

    res_bundle = run_bundle.compose_resource(
        spec='TIFF', root=path_root, resource_path='stack.tiff',
        resource_kwargs={})
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={'slice': 5})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={'slice': 10})
    event1 = desc_bundle.compose_event(
        data={'motor': 0, 'image': datum_doc1['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False},
        seq_num=1)
    event2 = desc_bundle.compose_event(
        data={'motor': 0, 'image': datum_doc2['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False},
        seq_num=2)
    event3 = desc_bundle_baseline.compose_event(
        data={'motor': 0},
        timestamps={'motor': 0},
        seq_num=1)

    primary_event_page = event_model.pack_event_page(event1, event2)
    baseline_event_page = event_model.pack_event_page(event3)
    bulk_events = {'primary': [event1, event2], 'baseline': [event3]}
    pages = event_model.bulk_events_to_event_pages(bulk_events)
    assert tuple(pages) == (primary_event_page, baseline_event_page)


def test_sanitize_doc():
    run_bundle = event_model.compose_run()
    desc_bundle = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'},
                   'image': {'shape': [512, 512], 'dtype': 'number',
                             'source': '...', 'external': 'FILESTORE:'}},
        name='primary')
    desc_bundle_baseline = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'}},
        name='baseline')
    event1 = desc_bundle.compose_event(
        data={'motor': 0, 'image': numpy.ones((512, 512))},
        timestamps={'motor': 0, 'image': 0}, filled={'image': True},
        seq_num=1)
    event2 = desc_bundle.compose_event(
        data={'motor': 0, 'image': numpy.ones((512, 512))},
        timestamps={'motor': 0, 'image': 0}, filled={'image': True},
        seq_num=2)
    event3 = desc_bundle_baseline.compose_event(
        data={'motor': 0},
        timestamps={'motor': 0},
        seq_num=1)

    event_page = event_model.pack_event_page(event1, event2)
    bulk_events = {'primary': [event1, event2], 'baseline': [event3]}
    json.dumps(event_model.sanitize_doc(event_page))
    json.dumps(event_model.sanitize_doc(bulk_events))
    json.dumps(event_model.sanitize_doc(event1))


def test_bulk_datum_to_datum_page():
    run_bundle = event_model.compose_run()
    res_bundle = run_bundle.compose_resource(
        spec='TIFF', root='/tmp', resource_path='stack.tiff',
        resource_kwargs={})
    datum1 = res_bundle.compose_datum(datum_kwargs={'slice': 5})
    datum2 = res_bundle.compose_datum(datum_kwargs={'slice': 10})

    actual = event_model.pack_datum_page(datum1, datum2)
    bulk_datum = {'resource': res_bundle.resource_doc['uid'],
                  'datum_kwarg_list': [datum1['datum_kwargs'],
                                       datum2['datum_kwargs']],
                  'datum_ids': [datum1['datum_id'], datum2['datum_id']]}
    expected = event_model.bulk_datum_to_datum_page(bulk_datum)
    assert actual == expected


def test_document_router_smoke_test():
    dr = event_model.DocumentRouter()
    run_bundle = event_model.compose_run()
    dr('start', run_bundle.start_doc)
    desc_bundle = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'},
                   'image': {'shape': [512, 512], 'dtype': 'number',
                             'source': '...', 'external': 'FILESTORE:'}},
        name='primary')
    dr('descriptor', desc_bundle.descriptor_doc)
    desc_bundle_baseline = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'}},
        name='baseline')
    dr('descriptor', desc_bundle_baseline.descriptor_doc)
    res_bundle = run_bundle.compose_resource(
        spec='TIFF', root='/tmp', resource_path='stack.tiff',
        resource_kwargs={})
    dr('resource', res_bundle.resource_doc)
    datum_doc1 = res_bundle.compose_datum(datum_kwargs={'slice': 5})
    datum_doc2 = res_bundle.compose_datum(datum_kwargs={'slice': 10})
    dr('datum', datum_doc1)
    dr('datum', datum_doc2)
    event1 = desc_bundle.compose_event(
        data={'motor': 0, 'image': datum_doc1['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False},
        seq_num=1)
    dr('event', event1)
    event2 = desc_bundle.compose_event(
        data={'motor': 0, 'image': datum_doc2['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False},
        seq_num=2)
    dr('event', event2)
    event3 = desc_bundle_baseline.compose_event(
        data={'motor': 0},
        timestamps={'motor': 0},
        seq_num=1)
    dr('event', event3)
    dr('stop', run_bundle.compose_stop())


def test_filler(tmp_path):

    class DummyHandler:
        def __init__(self, resource_path, a, b):
            assert a == 1
            assert b == 2
            assert resource_path == str(tmp_path / "stack.tiff")

        def __call__(self, c, d):
            assert c == 3
            assert d == 4
            return numpy.ones((5, 5))

    path_root = str(tmp_path)

    reg = {'DUMMY': DummyHandler}
    filler = event_model.Filler(reg)
    run_bundle = event_model.compose_run()
    desc_bundle = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'},
                   'image': {'shape': [512, 512], 'dtype': 'number',
                             'source': '...', 'external': 'FILESTORE:'}},
        name='primary')
    desc_bundle_baseline = run_bundle.compose_descriptor(
        data_keys={'motor': {'shape': [], 'dtype': 'number', 'source': '...'}},
        name='baseline')
    res_bundle = run_bundle.compose_resource(
        spec='DUMMY', root=path_root, resource_path='stack.tiff',
        resource_kwargs={'a': 1, 'b': 2})
    datum_doc = res_bundle.compose_datum(datum_kwargs={'c': 3, 'd': 4})
    raw_event = desc_bundle.compose_event(
        data={'motor': 0, 'image': datum_doc['datum_id']},
        timestamps={'motor': 0, 'image': 0}, filled={'image': False},
        seq_num=1)
    filler('start', run_bundle.start_doc)
    filler('descriptor', desc_bundle.descriptor_doc)
    filler('descriptor', desc_bundle_baseline.descriptor_doc)
    filler('resource', res_bundle.resource_doc)
    filler('datum', datum_doc)
    event = copy.deepcopy(raw_event)
    assert isinstance(event['data']['image'], str)
    filler('event', event)
    stop_doc = run_bundle.compose_stop()
    filler('stop', stop_doc)
    assert event['data']['image'].shape == (5, 5)
    assert not filler._closed
    filler.close()
    assert filler._closed

    # Test context manager
    with event_model.Filler(reg) as filler:
        filler('start', run_bundle.start_doc)
        filler('descriptor', desc_bundle.descriptor_doc)
        filler('descriptor', desc_bundle_baseline.descriptor_doc)
        filler('resource', res_bundle.resource_doc)
        filler('datum', datum_doc)
        event = copy.deepcopy(raw_event)
        name, doc = filler('event', event)
        assert name == 'event'
        assert doc is event
        filler('stop', stop_doc)
        assert not filler._closed
    assert event['data']['image'].shape == (5, 5)
    assert filler._closed

    # Test undefined handler spec
    with event_model.Filler({}) as filler:
        filler('start', run_bundle.start_doc)
        filler('descriptor', desc_bundle.descriptor_doc)
        filler('descriptor', desc_bundle_baseline.descriptor_doc)
        filler('resource', res_bundle.resource_doc)
        filler('datum', datum_doc)
        event = copy.deepcopy(raw_event)
        assert isinstance(event['data']['image'], str)
        with pytest.raises(event_model.UndefinedAssetSpecification):
            filler('event', event)

    # Test exclude and include.
    with pytest.raises(ValueError):
        event_model.Filler({}, include=[], exclude=[])

    with event_model.Filler(reg, exclude=['image']) as filler:
        filler('start', run_bundle.start_doc)
        filler('descriptor', desc_bundle.descriptor_doc)
        filler('descriptor', desc_bundle_baseline.descriptor_doc)
        filler('resource', res_bundle.resource_doc)
        filler('datum', datum_doc)
        event = copy.deepcopy(raw_event)
        assert isinstance(event['data']['image'], str)
        filler('event', event)
        filler('stop', stop_doc)

    with event_model.Filler(reg, include=['image']) as filler:
        filler('start', run_bundle.start_doc)
        filler('descriptor', desc_bundle.descriptor_doc)
        filler('descriptor', desc_bundle_baseline.descriptor_doc)
        filler('resource', res_bundle.resource_doc)
        filler('datum', datum_doc)
        event = copy.deepcopy(raw_event)
        filler('event', event)
        filler('stop', stop_doc)
        assert not filler._closed
    assert event['data']['image'].shape == (5, 5)

    with event_model.Filler(reg, include=['image', 'EXTRA THING']) as filler:
        filler('start', run_bundle.start_doc)
        filler('descriptor', desc_bundle.descriptor_doc)
        filler('descriptor', desc_bundle_baseline.descriptor_doc)
        filler('resource', res_bundle.resource_doc)
        filler('datum', datum_doc)
        event = copy.deepcopy(raw_event)
        filler('event', event)
        filler('stop', stop_doc)
        assert not filler._closed
    assert event['data']['image'].shape == (5, 5)

    class DummyHandlerRootMapTest:
        def __init__(self, resource_path, a, b):
            assert a == 1
            assert b == 2
            assert resource_path == str(tmp_path / "moved" / "stack.tiff")

        def __call__(self, c, d):
            assert c == 3
            assert d == 4
            return numpy.ones((5, 5))

    with event_model.Filler({'DUMMY': DummyHandlerRootMapTest},
                            root_map={path_root: str(tmp_path / "moved")}
                            ) as filler:

        filler('start', run_bundle.start_doc)
        filler('descriptor', desc_bundle.descriptor_doc)
        filler('descriptor', desc_bundle_baseline.descriptor_doc)
        filler('resource', res_bundle.resource_doc)
        filler('datum', datum_doc)
        event = copy.deepcopy(raw_event)
        filler('event', event)
        filler('stop', stop_doc)
        assert not filler._closed
    assert event['data']['image'].shape == (5, 5)

    # Test verify_filled.
    with pytest.raises(event_model.UnfilledData):
        event_model.verify_filled(event_model.pack_event_page(raw_event))
    event_model.verify_filled(event_model.pack_event_page(event))
