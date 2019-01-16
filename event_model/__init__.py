from collections import defaultdict, namedtuple
import json
import jsonschema
import numpy
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
        return (name, output_doc if output_doc is not None else doc)

    def start(self, doc):
        return doc

    def stop(self, doc):
        return doc

    def descriptor(self, doc):
        return doc

    def resource(self, doc):
        return doc

    def event(self, doc):
        event_page = pack_event_page(doc)
        output_event_page = self.event_page(event_page)
        output_event, = unpack_event_page(output_event_page)
        return output_event

    def datum(self, doc):
        datum_page = pack_datum_page(doc)
        output_datum_page = self.datum_page(datum_page)
        output_datum, = unpack_datum_page(output_datum_page)
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
        self.datum_page(bulk_datum_to_datum_page(doc))


class EventModelError(Exception):
    ...


class EventModelValueError(EventModelError, ValueError):
    ...


class EventModelValidationError(EventModelError):
    ...


class UnfilledData(EventModelError):
    """raised when unfilled data is found"""
    ...


SCHEMA_PATH = 'schemas'
SCHEMA_NAMES = {DocumentNames.start: 'schemas/run_start.json',
                DocumentNames.stop: 'schemas/run_stop.json',
                DocumentNames.event: 'schemas/event.json',
                DocumentNames.event_page: 'schemas/event_page.json',
                DocumentNames.descriptor: 'schemas/event_descriptor.json',
                DocumentNames.datum: 'schemas/datum.json',
                DocumentNames.datum_page: 'schemas/datum_page.json',
                DocumentNames.resource: 'schemas/resource.json',
                # DEPRECATED:
                DocumentNames.bulk_events: 'schemas/bulk_events.json',
                DocumentNames.bulk_datum: 'schemas/bulk_datum.json'}
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


def pack_event_page(*events):
    time_list = []
    uid_list = []
    seq_num_list = []
    data_list = []
    filled_list = []
    timestamps_list = []
    for event in events:
        time_list.append(event['time'])
        uid_list.append(event['uid'])
        seq_num_list.append(event['seq_num'])
        filled_list.append(event['filled'])
        data_list.append(event['data'])
        timestamps_list.append(event['timestamps'])
    event_page = {'time': time_list, 'uid': uid_list, 'seq_num': seq_num_list,
                  'descriptor': event['descriptor'],
                  'filled': _transpose_list_of_dicts(filled_list),
                  'data': _transpose_list_of_dicts(data_list),
                  'timestamps': _transpose_list_of_dicts(timestamps_list)}
    return event_page


def unpack_event_page(event_page):
    descriptor = event_page['descriptor']
    data_list = _transpose_dict_of_lists(event_page['data'])
    timestamps_list = _transpose_dict_of_lists(event_page['timestamps'])
    filled_list = _transpose_dict_of_lists(event_page['filled'])
    for uid, time, seq_num, data, timestamps, filled in zip(
            event_page['uid'],
            event_page['time'],
            event_page['seq_num'],
            data_list,
            timestamps_list,
            filled_list or [{}] * len(data_list)):
        event = {'descriptor': descriptor,
                 'uid': uid, 'time': time, 'seq_num': seq_num,
                 'data': data, 'timestamps': timestamps, 'filled': filled}
        yield event


def pack_datum_page(*datum):
    datum_id_list = []
    datum_kwarg_list = []
    for datum_ in datum:
        datum_id_list.append(datum_['datum_id'])
        datum_kwarg_list.append(datum_['datum_kwargs'])
    datum_page = {'resource': datum_['resource'],
                  'datum_id': datum_id_list,
                  'datum_kwargs': _transpose_list_of_dicts(datum_kwarg_list)}
    return datum_page


def unpack_datum_page(datum_page):
    resource = datum_page['resource']
    datum_kwarg_list = _transpose_dict_of_lists(datum_page['datum_kwargs'])
    for datum_id, datum_kwargs in zip(
            datum_page['datum_id'],
            datum_kwarg_list):
        datum = {'datum_id': datum_id, 'datum_kwargs': datum_kwargs,
                 'resource': resource}
        yield datum


def bulk_events_to_event_pages(bulk_events):
    # This is for a deprecated document type, so we are not being fussy
    # about efficiency/laziness here.
    event_pages = {}  # descriptor uid mapped to page
    for stream_name, events in bulk_events.items():
        for event in events:
            descriptor = event['descriptor']
            try:
                page = event_pages[descriptor]
            except KeyError:
                page = {'time': [], 'uid': [], 'seq_num': [],
                        'descriptor': descriptor}
                page['data'] = {k: [] for k in event['data']}
                page['timestamps'] = {k: [] for k in event['timestamps']}
                page['filled'] = {k: [] for k in event['filled']}
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
            page_filled = page['filled']
            for k, v in event['filled'].items():
                page_filled[k].append(v)
    return list(event_pages.values())


def bulk_datum_to_datum_page(bulk_datum):
    datum_page = {'datum_id': bulk_datum['datum_ids'],
                  'resource': bulk_datum['resource'],
                  'datum_kwargs': _transpose_list_of_dicts(
                      bulk_datum['datum_kwarg_list'])}
    return datum_page


def _transpose_list_of_dicts(list_of_dicts):
    "Transform list-of-dicts into dict-of-lists (i.e. DataFrame-like)."
    dict_of_lists = defaultdict(list)
    for row in list_of_dicts:
        for k, v in row.items():
            dict_of_lists[k].append(v)
    return dict(dict_of_lists)


def _transpose_dict_of_lists(dict_of_lists):
    "Transform dict-of-lists (i.e. DataFrame-like) into list-of-dicts."
    list_of_dicts = []
    keys = list(dict_of_lists)
    for row in zip(*(dict_of_lists[k] for k in keys)):
        list_of_dicts.append(dict(zip(keys, row)))
    return list_of_dicts


def verify_filled(event_page):
    '''Take an event_page document and verify that it is completely filled.

    Parameters
    ----------
    event_page : event_page document
        The event page document to check

    Raises
    ------
    UnfilledData
        Raised if any of the data in the event_page is unfilled, when raised it
        inlcudes a list of unfilled data objects in the exception message.
    '''

    if not all(map(all, event_page['filled'].values())):
        # check that all event_page data is filled.
        unfilled_data = []
        for field in event_page['filled']:
            if not event_page['filled'][field]:
                for field, filled in event_page['filled'].items():
                    if not all(filled):
                        unfilled_data.append(field)
                        # Note: As of this writing, this is a slightly
                        # aspirational error message, as event_model.Filler has
                        # not been merged yet. May need to be revisited if it
                        # is renamed or kept elsewhere in the end.
                        raise UnfilledData("unfilled data found in "
                                           "{!r}. Try passing the parameter "
                                           "`gen` through `event_model.Filler`"
                                           " first.".format(unfilled_data))


def sanitize_doc(doc):
    '''Return a copy with any numpy objects converted to built-in python types.

    This function takes in an event-model document and returns a copy with any
    numpy objects converted to buil-in python types. It is useful for sanitzing
    documents prior to sending to any consumer that does not recognise numpy
    types, such as a MongoDB database or a JSON encoder.

    Parameters
    ----------
    doc : event-model document.
        The event-model document to be sanitized

    Returns
    -------
    sanitized_doc : event-model document
        The event-model document with numpy objects converted to built-in pyton
        types.
    '''
    sanitized_doc = doc.copy()
    _apply_to_dict_recursively(doc, _sanitize_numpy)

    return sanitized_doc


def _sanitize_numpy(val):
    '''Convert any numpy objects into built-in Python types.

    Parameters
    ----------
    val : object
        The potential numpy object to be converted.

    Returns
    -------
    val : object
        The input parameter, converted to a built-in python type if it is a
        numpy type.
    '''
    if isinstance(val, (numpy.generic, numpy.ndarray)):
        if numpy.isscalar(val):
            return val.item()
        return val.tolist()
    return val


def _apply_to_dict_recursively(dictionary, func):
    '''Recursively and apply a function to a dictionary of dictionaries.

    Takes in a dictionary of dictionaries and applies a function to each value
    in the dictionary

    Parameters
    ----------
    dictionary : dict
        The dictionary of dictionaries to be recursivly searched.
    func : function
        A function to apply to each value in dictionary.
    '''

    for key, val in dictionary.items():
        if hasattr(val, 'items'):
            dictionary[key] = _apply_to_dict_recursively(val, func)
        dictionary[key] = func(val)
