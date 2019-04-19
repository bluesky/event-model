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
import numpy as np

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

    the document is passed to the method of the corresponding name, as in::

        getattr(router, name)(doc)

    The method is expected to return ``None`` or a valid document of the same
    type. It may be the original instance (passed through), a copy, or a
    different dict altogether.

    Finally, the call to ``router(name, doc)`` returns::

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
        # Subclass' implementation of event_page may return a valid EventPage
        # or None.
        if output_event_page is None:
            return None
        output_event, = unpack_event_page(output_event_page)
        return output_event

    def datum(self, doc):
        datum_page = pack_datum_page(doc)
        output_datum_page = self.datum_page(datum_page)
        # Subclass' implementation of event_page may return a valid DatumPage
        # or None.
        if output_datum_page is None:
            return None
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


class Filler(DocumentRouter):
    """Pass documents through, loading any externally-referenced data.

    It is recommended to use the Filler as a context manager.  Because the
    Filler manages caches of potentially expensive resources (e.g. large data
    in memory) managing its lifecycle is important. If used as a context
    manager, it will drop references to its caches upon exit from the
    context. Unless the user holds additional references to those caches, they
    will be garbage collected.

    But for some applications, such as taking multiple passes over the same
    data, it may be useful to keep a longer-lived Filler instance and then
    manually delete it when finished.

    See Examples below.

    Parameters
    ----------
    handler_registry : dict
        Maps each 'spec' (a string identifying a given type or external
        resource) to a handler class.

        A 'handler class' may be any callable with the signature::

            handler_class(resource_path, root, **resource_kwargs)

        It is expected to return an object, a 'handler instance', which is also
        callable and has the following signature::

            handler_instance(**datum_kwargs)

        As the names 'handler class' and 'handler instance' suggest, this is
        typically implemented using a class that implements ``__init__`` and
        ``__call__``, with the respective signatures. But in general it may be
        any callable-that-returns-a-callable.
    include : Iterable
        The set of fields to fill. By default all unfilled fields are filled.
        This parameter is mutually incompatible with the ``exclude`` parameter.
    exclude : Iterable
        The set of fields to skip filling. By default all unfilled fields are
        filled.  This parameter is mutually incompatible with the ``include``
        parameter.
    root_map: dict
        str -> str mapping to account for temporarily moved/copied/remounted
        files.  Any resources which have a ``root`` in ``root_map`` will be
        loaded using the mapped ``root``.
    handler_cache : dict, optional
        A cache of handler instances. If None, a dict is used.
    resource_cache : dict, optional
        A cache of Resource documents. If None, a dict is used.
    datum_cache : dict, optional
        A cache of Datum documents. If None, a dict is used.
    retry_intervals : Iterable, optional
        If data is not found on the first try, there may a race between the
        I/O systems creating the external data and this stream of Documents
        that reference it. If Filler encounters an ``IOError`` it will wait a
        bit and retry. This list specifies how long to sleep (in seconds)
        between subsequent attempts. Set to ``None`` to try only once before
        raising ``DataNotAccessible``. A subclass may catch this exception and
        implement a different retry mechanism --- for example using a different
        implementation of sleep from an async framework.  But by default, a
        sequence of several retries with increasing sleep intervals is used.
        The default sequence should not be considered stable; it may change at
        any time as the authors tune it.

    Raises
    ------
    DataNotAccessible
        If an IOError is raised when loading the data after the configured
        number of attempts. See the ``retry_intervals`` parameter for details.

    Examples
    --------
    A Filler may be used as a context manager.

    >>> with Filler(handler_registry) as filler:
    ...     for name, doc in stream:
    ...         filler(name, doc)  # mutates doc in place
    ...         # Do some analysis or export with name and doc.

    Or as a long-lived object.

    >>> f = Filler(handler_registry)
    >>> for name, doc in stream:
    ...     filler(name, doc)  # mutates doc in place
    ...     # Do some analysis or export with name and doc.
    ...
    >>> del filler  # Free up memory from potentially large caches.
    """
    def __init__(self, handler_registry, *,
                 include=None, exclude=None, root_map=None,
                 handler_cache=None, resource_cache=None, datum_cache=None,
                 retry_intervals=(0.001, 0.002, 0.004, 0.008, 0.016, 0.032,
                                  0.064, 0.128, 0.256, 0.512, 1.024)):
        if include is not None and exclude is not None:
            raise EventModelValueError(
                "The parameters `include` and `exclude` are mutually "
                "incompatible. At least one must be left as the default, "
                "None.")
        self.handler_registry = handler_registry
        self.include = include
        self.exclude = exclude
        self.root_map = root_map or {}
        if handler_cache is None:
            handler_cache = self.get_default_handler_cache()
        if resource_cache is None:
            resource_cache = self.get_default_resource_cache()
        if datum_cache is None:
            datum_cache = self.get_default_datum_cache()
        self._handler_cache = handler_cache
        self._resource_cache = resource_cache
        self._datum_cache = datum_cache
        if retry_intervals is None:
            retry_intervals = []
        self.retry_intervals = list(retry_intervals)
        self._closed = False

    def __repr__(self):
        return "<Filler>" if not self._closed else "<Closed Filler>"

    @staticmethod
    def get_default_resource_cache():
        return {}

    @staticmethod
    def get_default_datum_cache():
        return {}

    @staticmethod
    def get_default_handler_cache():
        return {}

    def start(self, doc):
        return doc

    def resource(self, doc):
        # Defer creating the handler instance until we actually need it, when
        # we fill the first Event field that requires this Resource.
        self._resource_cache[doc['uid']] = doc
        return doc

    # Handlers operate document-wise, so we'll explode pages into individual
    # documents.

    def datum_page(self, doc):
        datum = self.datum  # Avoid attribute lookup in hot loop.
        for datum_doc in unpack_datum_page(doc):
            datum(datum_doc)
        return doc

    def datum(self, doc):
        self._datum_cache[doc['datum_id']] = doc
        return doc

    def event_page(self, doc):
        # TODO We may be able to fill a page in place, and that may be more
        # efficient than unpacking the page in to Events, filling them, and the
        # re-packing a new page. But that seems tricky in general since the
        # page may be implemented as a DataFrame or dict, etc.

        event = self.event  # Avoid attribute lookup in hot loop.
        filled_events = []

        for event_doc in unpack_event_page(doc):
            filled_events.append(event(event_doc))
        return pack_event_page(*filled_events)

    def event(self, doc):
        for key, is_filled in doc['filled'].items():
            if self.exclude is not None and key in self.exclude:
                continue
            if self.include is not None and key not in self.include:
                continue
            if not is_filled:
                datum_id = doc['data'][key]
                # Look up the cached Datum doc.
                try:
                    datum_doc = self._datum_cache[datum_id]
                except KeyError as err:
                    err_with_key = UnresolvableForeignKeyError(
                        f"Event with uid {doc['uid']} refers to unknown Datum "
                        f"datum_id {datum_id}")
                    err_with_key.key = datum_id
                    raise err_with_key from err
                resource_uid = datum_doc['resource']
                # Look up the cached Resource.
                try:
                    resource = self._resource_cache[resource_uid]
                except KeyError as err:
                    raise UnresolvableForeignKeyError(
                        f"Datum with id {datum_id} refers to unknown Resource "
                        f"uid {resource_uid}") from err
                # Look up the cached handler instance, or instantiate one.
                try:
                    handler = self._handler_cache[resource['uid']]
                except KeyError:
                    try:
                        handler_class = self.handler_registry[resource['spec']]
                    except KeyError as err:
                        raise UndefinedAssetSpecification(
                            f"Resource document with uid {resource['uid']} "
                            f"refers to spec {resource['spec']!r} which is "
                            f"not defined in the Filler's "
                            f"handler registry.") from err
                    try:
                        # Apply root_map.
                        resource_path = resource['resource_path']
                        root = resource.get('root', '')
                        root = self.root_map.get(root, root)
                        if root:
                            resource_path = os.path.join(root, resource_path)

                        handler = handler_class(resource_path,
                                                **resource['resource_kwargs'])
                    except Exception as err:
                        raise EventModelError(
                            f"Error instantiating handler "
                            f"class {handler_class} "
                            f"with Resource document {resource}.") from err
                    self._handler_cache[resource['uid']] = handler

                # We are sure to attempt to read that data at least once and
                # then perhaps additional times depending on the contents of
                # retry_intervals.
                error = None
                for interval in [0] + self.retry_intervals:
                    ttime.sleep(interval)
                    try:
                        actual_data = handler(**datum_doc['datum_kwargs'])
                        doc['data'][key] = actual_data
                        doc['filled'][key] = datum_id
                    except IOError as error_:
                        # The file may not be visible on the network yet.
                        # Wait and try again. Stash the error in a variable
                        # that we can access later if we run out of attempts.
                        error = error_
                    else:
                        break
                else:
                    # We have used up all our attempts. There seems to be an
                    # actual problem. Raise the error stashed above.
                    raise DataNotAccessible(
                        f"Filler was unable to load the data referenced by "
                        f"the Datum document {datum_doc} and the Resource "
                        f"document {resource}.") from error
        return doc

    def descriptor(self, doc):
        return doc

    def stop(self, doc):
        return doc

    def __enter__(self):
        return self

    def close(self):
        # Drop references to the caches. If the user holds another reference to
        # them it's the user's problem to manage their lifecycle. If the user
        # does not (e.g. they are the default caches) the gc will look after
        # them.
        self._closed = True
        self._handler_cache = None
        self._resource_cache = None
        self._datum_cache = None

    def __exit__(self, *exc_details):
        self.close()

    def __call__(self, name, doc, validate=False):
        if self._closed:
            raise EventModelRuntimeError(
                "This Filler has been closed and is no longer usable.")
        return super().__call__(name, doc, validate)


class EventModelError(Exception):
    ...


class EventModelKeyError(EventModelError, KeyError):
    ...


class EventModelValueError(EventModelError, ValueError):
    ...


class EventModelRuntimeError(EventModelError, RuntimeError):
    ...


class EventModelValidationError(EventModelError):
    ...


class UnfilledData(EventModelError):
    """raised when unfilled data is found"""
    ...


class UndefinedAssetSpecification(EventModelKeyError):
    """raised when a resource spec is missing from the handler registry"""
    ...


class DataNotAccessible(EventModelError, IOError):
    """raised when attempts to load data referenced by Datum document fail"""
    ...


class UnresolvableForeignKeyError(EventModelValueError):
    """when we see a foreign before we see the thing to which it refers"""
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
                                     'descriptor_doc compose_event compose_event_page')
ComposeResourceBundle = namedtuple('ComposeResourceBundle',
                                   'resource_doc compose_datum compose_datum_page')


def compose_datum(*, resource, counter, datum_kwargs, validate=True):
    resource_uid = resource['uid']
    doc = {'resource': resource_uid,
           'datum_kwargs': datum_kwargs,
           'datum_id': '{}/{}'.format(resource_uid, next(counter))}
    if validate:
        jsonschema.validate(doc, schemas[DocumentNames.datum])
    return doc


def compose_datum_page(*, resource, counter, datum_kwargs, validate=True):
    resource_uid = resource['uid']
    any_column, *_ = datum_kwargs.values()
    N = len(any_column)
    doc = {'resource': resource_uid,
           'datum_kwargs': datum_kwargs,
           'datum_id': ['{}/{}'.format(resource_uid, next(counter)) for _ in range(N)]}
    if validate:
        jsonschema.validate(doc, schemas[DocumentNames.datum])
    return doc


default_path_semantics = {'posix': 'posix', 'nt': 'windows'}[os.name]


def compose_resource(*, start, spec, root, resource_path, resource_kwargs,
                     path_semantics=default_path_semantics, uid=None, validate=True):
    if uid is None:
        uid = str(uuid.uuid4())
    counter = itertools.count()
    doc = {'uid': uid,
           'run_start': start['uid'],
           'spec': spec,
           'root': root,
           'resource_path': resource_path,
           'resource_kwargs': resource_kwargs,
           'path_semantics': path_semantics}
    if validate:
        jsonschema.validate(doc, schemas[DocumentNames.resource])
    return ComposeResourceBundle(
        doc,
        partial(compose_datum, resource=doc, counter=counter),
        partial(compose_datum_page, resource=doc, counter=counter))


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


def compose_event_page(*, descriptor, event_counter, data, timestamps, seq_num,
                       filled=None, uid=None, time=None, validate=True):
    N = len(seq_num)
    if uid is None:
        uid = [str(uuid.uuid4()) for _ in range(N)]
    if time is None:
        time = [ttime.time()] * N
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
        jsonschema.validate(doc, schemas[DocumentNames.event_page])
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


def compose_event(*, descriptor, event_counter, data, timestamps, seq_num=None,
                  filled=None, uid=None, time=None, validate=True):
    if seq_num is None:
        seq_num = event_counter[descriptor['name']]
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
                       object_keys=None, configuration=None, hints=None,
                       validate=True):
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    if object_keys is None:
        object_keys = {}
    if configuration is None:
        configuration = {}
    if hints is None:
        hints = {}
    doc = {'uid': uid,
           'time': time,
           'run_start': start['uid'],
           'name': name,
           'data_keys': data_keys,
           'object_keys': object_keys,
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
        partial(compose_event, descriptor=doc, event_counter=event_counter),
        partial(compose_event_page, descriptor=doc, event_counter=event_counter))


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
    """
    Transform one or more Event documents into an EventPage document.

    Parameters
    ----------
    *event : dicts
        any number of Event documents

    Returns
    -------
    event_page : dict
    """
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
        filled_list.append(event.get('filled', {}))
        data_list.append(event['data'])
        timestamps_list.append(event['timestamps'])
    event_page = {'time': time_list, 'uid': uid_list, 'seq_num': seq_num_list,
                  'descriptor': event['descriptor'],
                  'filled': _transpose_list_of_dicts(filled_list),
                  'data': _transpose_list_of_dicts(data_list),
                  'timestamps': _transpose_list_of_dicts(timestamps_list)}
    return event_page


def unpack_event_page(event_page):
    """
    Transform an EventPage document into individual Event documents.

    Parameters
    ----------
    event_page : dict

    Yields
    ------
    event : dict
    """
    descriptor = event_page['descriptor']
    data_list = _transpose_dict_of_lists(event_page['data'])
    timestamps_list = _transpose_dict_of_lists(event_page['timestamps'])
    filled_list = _transpose_dict_of_lists(event_page.get('filled', {}))
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
    """
    Transform one or more Datum documents into a DatumPage document.

    Parameters
    ----------
    *datum : dicts
        any number of Datum documents

    Returns
    -------
    datum_page : dict
    """
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
    """
    Transform a DatumPage document into individual Datum documents.

    Parameters
    ----------
    datum_page : dict

    Yields
    ------
    datum : dict
    """
    resource = datum_page['resource']
    datum_kwarg_list = _transpose_dict_of_lists(datum_page['datum_kwargs'])
    for datum_id, datum_kwargs in zip(
            datum_page['datum_id'],
            datum_kwarg_list):
        datum = {'datum_id': datum_id, 'datum_kwargs': datum_kwargs,
                 'resource': resource}
        yield datum


def bulk_events_to_event_pages(bulk_events):
    """
    Transform a BulkEvents document into a list of EventPage documents.

    Note: The BulkEvents layout has been deprecated in favor of EventPage.

    Parameters
    ----------
    bulk_events : dict

    Returns
    -------
    event_pages : list
    """
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
    """
    Transform one BulkDatum into one DatumPage.

    Note: There is only one known usage of BulkDatum "in the wild", and the
    BulkDatum layout has been deprecated in favor of DatumPage.
    """
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
        for field, filled in event_page['filled'].items():
            if not all(filled):
                unfilled_data.append(field)
                raise UnfilledData(f"Unfilled data found in fields "
                                   f"{unfilled_data!r}. Use "
                                   f"`event_model.Filler`.")


def sanitize_np(doc):
    '''Return a copy with any numpy objects converted to built-in Python types.
    This is a faster version of sanitize_doc which only converts numpy objects.

    This function takes in an event-model document and returns a copy with any
    numpy objects converted to built-in Python types. It is useful for
    sanitizing documents prior to sending to any consumer that does not
    recognize numpy types, such as a MongoDB database or a JSON encoder.

    Parameters
    ----------
    doc : dict
        The event-model document to be sanitized

    Returns
    -------
    sanitized_doc : event-model document
        The event-model document with numpy objects converted to built-in
        Python types.
    '''
    return {key: sanitize_item(value) for key, value in doc.items()}


def sanitize_item(val):
    "Convert any numpy objects into built-in Python types."
    if isinstance(val, (np.generic, np.ndarray)):
        if np.isscalar(val):
            return val.item()
        return val.tolist()
    return val


def sanitize_doc(doc):
    '''Return a copy with any numpy objects converted to built-in Python types.

    This function takes in an event-model document and returns a copy with any
    numpy objects converted to built-in Python types. It is useful for
    sanitizing documents prior to sending to any consumer that does not
    recognize numpy types, such as a MongoDB database or a JSON encoder.

    Parameters
    ----------
    doc : dict
        The event-model document to be sanitized

    Returns
    -------
    sanitized_doc : event-model document
        The event-model document with numpy objects converted to built-in
        Python types.
    '''
    return json.loads(json.dumps(doc, cls=NumpyEncoder))


class NumpyEncoder(json.JSONEncoder):
    """
    A json.JSONEncoder for encoding numpy objects using built-in Python types.

    Examples
    --------

    Encode a Python object that includes an arbitrarily-nested numpy object.

    >>> json.dumps({'a': {'b': numpy.array([1, 2, 3])}}, cls=NumpyEncoder)
    """
    # Credit: https://stackoverflow.com/a/47626762/1221924
    def default(self, obj):
        if isinstance(obj, (numpy.generic, numpy.ndarray)):
            if numpy.isscalar(obj):
                return obj.item()
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
