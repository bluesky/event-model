from collections import namedtuple
import json
import jsonschema
from enum import Enum
from functools import partial
import itertools
import os
from pkg_resources import resource_filename as rs_fn
import time as ttime
import uuid
import warnings
from ._version import get_versions

__all__ = ['DocumentNames', 'schemas', 'compose_run']

_validate = partial(jsonschema.validate, types={'array': (list, tuple)})


class DocumentNames(Enum):
    stop = 'stop'
    start = 'start'
    descriptor = 'descriptor'
    event = 'event'
    datum = 'datum'
    resource = 'resource'
    event_page = 'event_page'
    datum_page = 'datum_page'
    bulk_datum = 'bulk_datum'  # deprecated
    bulk_events = 'bulk_events'  # deprecated


class DocumentRouter:
    """
    Route each document by type to a corresponding method.

    When an instance is called with a document type and a document like::

        router(name, doc)

    the document is passed to the method of the corresponding name, as in

        getattr(router, name)(doc)

    The method is expected to return a valid document of the same type. It may
    be the original instance (passed through), a copy, or a different dict
    altogether.

    Finally, the call to ``router(name, doc))`` returns::

        (name, getattr(router, name)(doc))
    """
    def __call__(self, name, doc, validate=False):
        """
        Process a document.

        Parameters
        ----------
        name : string
        doc : dict
        validate : boolean
            Apply jsonschema validation to the documents coming *out*. This is
            False by default.

        Returns
        -------
        name, output_doc : string, dict
            The same name as what was passed in, and a doc that may be the same
            instance as doc, a copy of doc, or a different dict altogether.
        """
        output_doc = getattr(self, name)(doc)
        if validate:
            jsonschema.validate(output_doc,
                                schemas[getattr(DocumentNames, name)])
        return (name, output_doc) if output_doc is not None else output_doc

    def start(self, doc):
        return doc

    def stop(self, doc):
        return doc

    def descriptor(self, doc):
        return doc

    def resource(self, doc):
        return doc

    def event(self, doc):
        event_page = pack_event_into_event_page(doc)
        output_event_page = self.event_page(event_page)
        output_event = unpack_event_page_into_event(output_event_page)
        return output_event

    def datum(self, doc):
        datum_page = pack_datum_into_datum_page(doc)
        output_datum_page = self.datum_page(datum_page)
        output_datum = unpack_datum_page_into_datum(output_datum_page)
        return output_datum

    def event_page(self, doc):
        return doc

    def datum_page(self, doc):
        return doc

    def bulk_events(self, doc):
        # Do not modify this in a subclass. Use event_page.
        warnings.warn(
            "The document type 'bulk_events' has been deprecated in favor of "
            "'event_page', whose structure is a transpose of 'bulk_events'.")
        for page in bulk_events_to_event_pages(doc):
            self.event_page(page)

    def bulk_datum(self, doc):
        # Do not modify this in a subclass. Use event_page.
        warnings.warn(
            "The document type 'bulk_datum' has been deprecated in favor of "
            "'datum_page', whose structure is a transpose of 'bulk_datum'.")
        for page in bulk_datum_to_datum_page(doc):
            self.datum_page(page)


class EventModelError(Exception):
    ...


class EventModelValueError(EventModelError, ValueError):
    ...


class EventModelValidationError(EventModelError):
    ...


SCHEMA_PATH = 'schemas'
SCHEMA_NAMES = {DocumentNames.start: 'schemas/run_start.json',
                DocumentNames.stop: 'schemas/run_stop.json',
                DocumentNames.event: 'schemas/event.json',
                DocumentNames.bulk_events: 'schemas/bulk_events.json',
                DocumentNames.descriptor: 'schemas/event_descriptor.json',
                DocumentNames.datum: 'schemas/datum.json',
                DocumentNames.resource: 'schemas/resource.json'}
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    with open(rs_fn('event_model', filename)) as fin:
        schemas[name] = json.load(fin)

__version__ = get_versions()['version']
del get_versions

ComposeRunBundle = namedtuple('ComposeRunBundle',
                              'start_doc compose_descriptor compose_resource '
                              'compose_stop')
ComposeDescriptorBundle = namedtuple('ComposeDescriptorBundle',
                                     'descriptor_doc compose_event')
ComposeResourceBundle = namedtuple('ComposeResourceBundle',
                                   'resource_doc compose_datum')


def compose_datum(*, resource, counter, datum_kwargs, validate=True):
    resource_uid = resource['uid']
    doc = {'resource': resource_uid,
           'datum_kwargs': datum_kwargs,
           'datum_id': '{}/{}'.format(resource_uid, next(counter))}
    if validate:
        jsonschema.validate(doc, schemas[DocumentNames.datum])
    return doc


def compose_resource(*, start, spec, root, resource_path, resource_kwargs,
                     path_semantics=os.name, uid=None, validate=True):
    if uid is None:
        uid = str(uuid.uuid4())
    counter = itertools.count()
    doc = {'uid': uid,
           'run_start': start['uid'],
           'spec': spec,
           'root': root,
           'resource_path': resource_path,
           'resource_kwargs': {},
           'path_semantics': path_semantics}
    if validate:
        jsonschema.validate(doc, schemas[DocumentNames.resource])
    return ComposeResourceBundle(
        doc,
        partial(compose_datum, resource=doc, counter=counter))


def compose_stop(*, start, event_counter, poison_pill,
                 exit_status='success', reason='',
                 uid=None, time=None,
                 validate=True):
    if poison_pill:
        raise EventModelError("Already composed a RunStop document for run "
                              "{!r}.".format(start['uid']))
    poison_pill.append(object())
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    doc = {'uid': uid,
           'time': time,
           'run_start': start['uid'],
           'exit_status': exit_status,
           'reason': reason,
           'num_events': dict(event_counter)}
    if validate:
        jsonschema.validate(doc, schemas[DocumentNames.stop])
    return doc


def compose_event(*, descriptor, event_counter, data, timestamps, seq_num,
                  filled=None, uid=None, time=None, validate=True):
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    if filled is None:
        filled = {}
    doc = {'uid': uid,
           'time': time,
           'data': data,
           'timestamps': timestamps,
           'seq_num': seq_num,
           'filled': filled,
           'descriptor': descriptor['uid']}
    if validate:
        jsonschema.validate(doc, schemas[DocumentNames.event])
        if not (descriptor['data_keys'].keys() == data.keys() == timestamps.keys()):
            raise EventModelValidationError(
                "These sets of keys must match:\n"
                "event['data'].keys(): {}\n"
                "event['timestamps'].keys(): {}\n"
                "descriptor['data_keys'].keys(): {}\n".format(
                    data.keys(), timestamps.keys(), descriptor['data_keys'].keys()))
        if set(filled) - set(data):
            raise EventModelValidationError(
                "Keys in event['filled'] {} must be a subset of those in "
                "event['data'] {}".format(filled.keys(), data.keys()))
    event_counter[descriptor['name']] += 1
    return doc


def compose_descriptor(*, start, streams, event_counter,
                       name, data_keys, uid=None, time=None,
                       object_names=None, configuration=None, hints=None,
                       validate=True):
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    if object_names is None:
        object_names = {}
    if configuration is None:
        configuration = {}
    if hints is None:
        hints = {}
    doc = {'uid': uid,
           'time': time,
           'run_start': start['uid'],
           'name': name,
           'data_keys': data_keys,
           'object_names': object_names,
           'hints': hints,
           'configuration': configuration}
    if validate:
        if name in streams and streams[name] != set(data_keys):
            raise EventModelValidationError(
                "A descriptor with the name {} has already been composed with "
                "data_keys {}. The requested data_keys were {}. All "
                "descriptors in a given stream must have the same "
                "data_keys.".format(name, streams[name], set(data_keys)))
        jsonschema.validate(doc, schemas[DocumentNames.descriptor])
    if name not in streams:
        streams[name] = set(data_keys)
        event_counter[name] = 0
    return ComposeDescriptorBundle(
        doc,
        partial(compose_event, descriptor=doc, event_counter=event_counter))


def compose_run(*, uid=None, time=None, metadata=None, validate=True):
    """
    Compose a RunStart document and factory functions for related documents.

    Parameters
    ----------
    uid : string, optional
        Unique identifier for this run, conventionally a UUID4. If None is
        given, a UUID4 will be generated.
    time: float, optional
        UNIX epoch time of start of this run. If None is given, the current
        time will be used.
    metadata : dict, optional
        Additional metadata include the document
    validate : boolean, optional
        Validate this document conforms to the schema.
    """
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    if metadata is None:
        metadata = {}
    doc = dict(uid=uid, time=time, **metadata)
    # Define some mutable state to be shared internally by the closures composed
    # below.
    streams = {}
    event_counter = {}
    poison_pill = []
    if validate:
        jsonschema.validate(doc, schemas[DocumentNames.start])
    return ComposeRunBundle(
        doc,
        partial(compose_descriptor, start=doc, streams=streams, event_counter={}),
        partial(compose_resource, start=doc),
        partial(compose_stop, start=doc, event_counter=event_counter,
                poison_pill=poison_pill))


def pack_event_into_event_page(event):
    return {'descriptor': event['descriptor'],
            'time': [event['time']],
            'seq_num': [event['seq_num']],
            'uid': [event['uid']],
            'data': {key: [val] for key, val in event['data'].items()},
            'timestamps': {key: [val] for key, val in event['timestamps'].items()},
            'filled': {key: [val] for key, val in event['filled'].items()}}


def unpack_event_page_into_event(event_page):
    event = {'descriptor': event_page['descriptor']}
    # Use sequence unpacking to validate that event_page has length 1.
    try:
        event['uid'], = event_page['uid']
        event['time'], = event_page['time']
        event['seq_num'], = event_page['seq_num']
        event['data'] = {k: v for (k, (v,)) in event_page['data'].items()}
        event['timestamps'] = {k: v for (k, (v,)) in event_page['timestamps'].items()}
        event['filled'] = {k: v for (k, (v,)) in event_page['filled'].items()}
    except ValueError:
        raise EventModelValueError(
            f"Cannot convert event_page to single event "
            f"unless page length is 1. Erroneous event_page is: {event_page}")
    return event


def pack_datum_into_datum_page(datum):
    return {'resource': datum['resource'],
            'datum_id': [datum['datum_id']],
            'datum_kwargs': {key: [val] for key, val in datum['datum_kwargs'].items()}}


def unpack_datum_page_into_datum(datum_page):
    datum = {'resource': datum_page['resource']}
    # Use sequence unpacking to validate that datum_page has length 1.
    try:
        datum['datum_id'], = datum_page['datum_id']
        datum['datum_kwargs'] = {k: v for (k, (v,)) in datum_page['datum_kwargs'].items()}
    except ValueError:
        raise EventModelValueError(
            f"Cannot convert datum_page to single datum "
            f"unless page length is 1. Erroneous datum_page is: {datum_page}")
    return datum


def bulk_events_to_event_pages(bulk_events):
    # This is for a deprecated document type, so we are not being fussy
    # about efficiency/laziness here.
    event_pages = {}  # descriptor uid mapped to page
    for stream_name, event in bulk_events.items():
        descriptor = event['descriptor']
        try:
            page = event_pages[descriptor]
        except KeyError:
            page = {'time': [], 'uid': [], 'seq_num': [],
                    'descriptor': descriptor}
            page['data'] = {k: [] for k in event['data']}
            page['timestamps'] = {k: [] for k in event['timestamps']}
            event_pages[descriptor] = page
        page['uid'].append(event['uid'])
        page['time'].append(event['time'])
        page['seq_num'].append(event['seq_num'])
        page_data = page['data']
        for k, v in event['data'].items():
            page_data[k].append(v)
        page_timestamps = page['timestamps']
        for k, v in event['timestamps'].items():
            page_timestamps[k].append(v)
    return list(event_pages.values())


def bulk_datum_to_datum_page(bulk_datum):
    return {'datum_id': [datum['datum_id'] for datum in bulk_datum],
            'datum_kwargs': [datum['datum_kwargs'] for datum in bulk_datum],
            'resource': [datum['resource'] for datum in bulk_datum]}
