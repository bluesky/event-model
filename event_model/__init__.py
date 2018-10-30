import json
import jsonschema
from enum import Enum
from functools import partial
import itertools
import os
from pkg_resources import resource_filename as rs_fn
import time as ttime
import uuid
from ._version import get_versions

__all__ = ['DocumentNames', 'schemas', 'create_run']


class DocumentNames(Enum):
    stop = 'stop'
    start = 'start'
    descriptor = 'descriptor'
    event = 'event'
    bulk_events = 'bulk_events'
    datum = 'datum'
    resource = 'resource'


class EventModelError(Exception):
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


def create_datum(*, resource, counter, datum_kwargs, validate=True):
    resource_uid = resource['uid']
    doc = {'resource': resource_uid,
           'datum_kwargs': datum_kwargs,
           'datum_id': '{}/{}'.format(resource_uid, next(counter))}
    if validate:
        jsonschema.validate(DocumentNames.datum, doc)
    return doc


def create_resource(*, start, spec, root, resource_path, resource_kwargs,
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
        jsonschema.validate(DocumentNames.resource, doc)
    return (doc,
            partial(create_datum, resource=doc, counter=counter))


def create_stop(*, start, event_counter, poison_pill,
                exit_status='success', reason='',
                uid=None, time=None,
                validate=True):
    if poison_pill:
        raise EventModelError("Already created a RunStop document for run "
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
        jsonschema.validate(DocumentNames.stop, doc)
    return doc


def create_event(*, descriptor, event_counter, data, timestamps, seq_num,
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
           'filled': filled}
    if validate:
        jsonschema.validate(DocumentNames.event, doc)
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


def create_descriptor(*, start, streams, event_counter,
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
        jsonschema.validate(DocumentNames.descriptor, doc)
        if name in streams and streams[name] != set(data_keys):
            raise EventModelValidationError(
                "A descriptor with the name {} has already been created with "
                "data_keys {}. The requested data_keys were {}. All "
                "descriptors in a given stream must have the same "
                "data_keys.".format(name, streams[name], set(data_keys)))
    if name not in streams:
        streams[name] = set(data_keys)
        event_counter[name] = 0
    return (doc,
            partial(create_event, descriptor=doc, event_counter=event_counter))


def create_run(*, uid=None, time=None, metadata=None, validate=True):
    """
    Create a RunStart document and factory functions for related documents.
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
    # Define some mutable state to be shared internally by the closures created
    # below.
    streams = {}
    event_counter = {}
    poison_pill = []
    if validate:
        jsonschema.validate(DocumentNames.start, doc)
    return (doc,
            partial(create_descriptor, start=doc, streams=streams, event_counter={}),
            partial(create_resource, start=doc),
            partial(create_stop, start=doc, event_counter=event_counter,
                    poison_pill=poison_pill))
