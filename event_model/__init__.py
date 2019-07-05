from collections import defaultdict, deque, namedtuple
import copy
import json
from enum import Enum
from functools import partial
import itertools
import os
from pkg_resources import resource_filename as rs_fn
import time as ttime
import uuid
import warnings

import jsonschema
import numpy

from ._version import get_versions

__all__ = ['DocumentNames', 'schemas', 'schema_validators', 'compose_run']


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
            schema_validators[DocumentNames.name].validate(output_doc)
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


class RunRouter(DocumentRouter):
    """
    Routes documents, by run, to callbacks it creates from factory functions.

    A RunRouter is callable, and it has the signature ``router(name, doc)``,
    suitable for subscribing to the RunEngine.

    It is configured with a list of factory functions that produce callbacks in
    a two-layered scheme, described below.

    .. warning::

       This is experimental. In a future release, it may be changed in a
       backward-incompatible way or fully removed.

    Parameters
    ----------
    factories : list
        A list of callables with the signature::

            factory('start', start_doc) -> List[Callbacks], List[SubFactories]

        which should return two lists, which may be empty. All items in the
        first list should be callbacks --- callables with the signature::

            callback(name, doc)

        that will receive all subsequent documents from the run including the
        RunStop document. All items in the second list should be "subfactories"
        with the signature::

            subfactory('descriptor', descriptor_doc) -> List[Callbacks]

        These will receive each of the EventDescriptor documents for the run,
        as they arrive. They must return one list, which may be empty,
        containing callbacks that will receive all Events that reference that
        EventDescriptor and finally the RunStop document for the run.
    """
    def __init__(self, factories):
        self.factories = factories

        # Map RunStart UID to "subfactory" functions that want all
        # EventDescriptors from that run.
        self._subfactories = defaultdict(list)

        # Callbacks that want all the documents from a given run, keyed on
        # RunStart UID.
        self._factory_cbs_by_start = defaultdict(list)

        # Callbacks that want all the documents from a given run, keyed on
        # each EventDescriptor UID in the run.
        self._factory_cbs_by_descriptor = defaultdict(list)

        # Callbacks that want documents related to a given EventDescriptor,
        # keyed on EventDescriptor UID.
        self._subfactory_cbs_by_descriptor = defaultdict(list)

        # Callbacks that want documents related to a given EventDescriptor,
        # keyed on the RunStart UID referenced by that EventDescriptor.
        self._subfactory_cbs_by_start = defaultdict(list)

        # Map RunStart UID to the list EventDescriptor. This is used to
        # facilitate efficient cleanup of the caches above.
        self._descriptors = defaultdict(list)

        # Map Resource UID to RunStart UID.
        self._resources = {}

        # Old-style Resources that do not have a RunStart UID
        self._unlabeled_resources = deque(maxlen=10000)

    def __repr__(self):
        return (f"RunRouter([\n" +
                f"\n".join(f"    {factory}" for factory in self.factories) +
                f"])")

    def start(self, doc):
        uid = doc['uid']
        for factory in self.factories:
            callbacks, subfactories = factory('start', doc)
            self._factory_cbs_by_start[uid].extend(callbacks)
            self._subfactories[uid].extend(subfactories)

    def descriptor(self, doc):
        uid = doc['uid']
        start_uid = doc['run_start']
        # Apply all factory cbs for this run to this descriptor, and run them.
        factory_cbs = self._factory_cbs_by_start[start_uid]
        self._factory_cbs_by_descriptor[uid].extend(factory_cbs)
        for callback in factory_cbs:
            callback('descriptor', doc)
        # Let all the subfactories add any relavant callbacks.
        for subfactory in self._subfactories[start_uid]:
            callbacks = subfactory('descriptor', doc)
            self._subfactory_cbs_by_start[start_uid].extend(callbacks)
            self._subfactory_cbs_by_descriptor[uid].extend(callbacks)
        # Keep track of the RunStart UID -> [EventDescriptor UIDs] mapping for
        # purposes of cleanup in stop().
        self._descriptors[start_uid].append(uid)

    def event_page(self, doc):
        descriptor_uid = doc['descriptor']
        for callback in self._factory_cbs_by_descriptor[descriptor_uid]:
            callback('event_page', doc)
        for callback in self._subfactory_cbs_by_descriptor[descriptor_uid]:
            callback('event_page', doc)

    def datum_page(self, doc):
        resource_uid = doc['resource']
        try:
            start_uid = self._resources[resource_uid]
        except KeyError:
            if resource_uid in self._unlabeled_resources:
                # Old Resources do not have a reference to a RunStart document,
                # so in turn we cannot immediately tell which run these datum
                # documents belong to.
                # Fan them out to every run currently flowing through RunRouter. If
                # they are not applicable they will do no harm, and this is
                # expected to be an increasingly rare case.
                for callbacks in self._factory_cbs_by_start.values():
                    for callback in callbacks:
                        callback('datum_page', doc)
                for callbacks in self._subfactory_cbs_by_start.values():
                    for callback in callbacks:
                        callback('datum_page', doc)
        else:
            for callback in self._factory_cbs_by_start[start_uid]:
                callback('datum_page', doc)
            for callback in self._subfactory_cbs_by_start[start_uid]:
                callback('datum_page', doc)

    def resource(self, doc):
        try:
            start_uid = doc['run_start']
        except KeyError:
            # Old Resources do not have a reference to a RunStart document.
            # Fan them out to every run currently flowing through RunRouter. If
            # they are not applicable they will do no harm, and this is
            # expected to be an increasingly rare case.
            self._unlabeled_resources.append(doc['uid'])
            for callbacks in self._factory_cbs_by_start.values():
                for callback in callbacks:
                    callback('resource', doc)
            for callbacks in self._subfactory_cbs_by_start.values():
                for callback in callbacks:
                    callback('resource', doc)
        else:
            self._resources[doc['uid']] = doc['run_start']
            for callback in self._factory_cbs_by_start[start_uid]:
                callback('resource', doc)
            for callback in self._subfactory_cbs_by_start[start_uid]:
                callback('resource', doc)

    def stop(self, doc):
        start_uid = doc['run_start']
        for callback in self._factory_cbs_by_start[start_uid]:
            callback('stop', doc)
        for callback in self._subfactory_cbs_by_start[start_uid]:
            callback('stop', doc)
        # Clean up references.
        self._subfactories.pop(start_uid, None)
        self._factory_cbs_by_start.pop(start_uid, None)
        self._subfactory_cbs_by_start.pop(start_uid, None)
        for descriptor_uid in self._descriptors.pop(start_uid, ()):
            self._factory_cbs_by_descriptor.pop(descriptor_uid, None)
            self._subfactory_cbs_by_descriptor.pop(descriptor_uid, None)
        self._resources.pop(start_uid, None)


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
    descriptor_cache : dict, optional
        A cache of EventDescriptor documents. If None, a dict is used.
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
                 descriptor_cache=None, inplace=None,
                 retry_intervals=(0.001, 0.002, 0.004, 0.008, 0.016, 0.032,
                                  0.064, 0.128, 0.256, 0.512, 1.024)):

        if inplace is None:
            self._inplace = True
            warnings.warn(
                "'inplace' argument not specified. It is recommended to "
                "specify True or False. In future releases, 'inplace' "
                "will default to False.")
        else:
            self._inplace = inplace

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
        if descriptor_cache is None:
            descriptor_cache = self.get_default_descriptor_cache()
        self._handler_cache = handler_cache
        self._resource_cache = resource_cache
        self._datum_cache = datum_cache
        self._descriptor_cache = descriptor_cache
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
    def get_default_descriptor_cache():
        return {}

    @staticmethod
    def get_default_datum_cache():
        return {}

    @staticmethod
    def get_default_handler_cache():
        return {}

    @property
    def inplace(self):
        return self._inplace

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
        filled_doc = self.fill_event_page(doc, include=self.include,
                                          exclude=self.exclude)
        return filled_doc

    def event(self, doc):
        filled_doc = self.fill_event(doc, include=self.include,
                                     exclude=self.exclude)
        return filled_doc

    def fill_event_page(self, doc, include=None, exclude=None, inplace=None):
        filled_events = []
        for event_doc in unpack_event_page(doc):
            filled_events.append(self.fill_event(event_doc,
                                                 include=include,
                                                 exclude=exclude,
                                                 inplace=True))
        filled_doc = pack_event_page(*filled_events)
        if inplace is None:
            inplace = self._inplace
        if inplace:
            doc['data'] = filled_doc['data']
            doc['filled'] = filled_doc['filled']
            return doc
        else:
            return filled_doc

    def fill_event(self, doc, include=None, exclude=None, inplace=None):
        if inplace is None:
            inplace = self._inplace
        if inplace:
            filled_doc = doc
        else:
            filled_doc = copy.deepcopy(doc)

        try:
            filled = doc['filled']
        except KeyError:
            # This document is not telling us which, if any, keys are filled.
            # Infer that none of the external data is filled.
            descriptor = self._descriptor_cache[doc['descriptor']]
            filled = {key: 'external' in val
                      for key, val in descriptor['data_keys'].items()}
        for key, is_filled in filled.items():
            if exclude is not None and key in exclude:
                continue
            if include is not None and key not in include:
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
                        # Here we are intentionally modifying doc in place.
                        filled_doc['data'][key] = actual_data
                        filled_doc['filled'][key] = datum_id
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
        return filled_doc

    def descriptor(self, doc):
        self._descriptor_cache[doc['uid']] = doc
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


def _is_array(checker, instance):
    return (
        jsonschema.validators.Draft7Validator.TYPE_CHECKER.is_type(instance, 'array') or
        isinstance(instance, tuple)
    )


_array_type_checker = jsonschema.validators.Draft7Validator.TYPE_CHECKER.redefine('array', _is_array)


_Validator = jsonschema.validators.extend(
    jsonschema.validators.Draft7Validator,
    type_checker=_array_type_checker)


schema_validators = {name: _Validator(schema=schema) for name, schema in schemas.items()}


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
        schema_validators[DocumentNames.datum].validate(doc)
    return doc


def compose_datum_page(*, resource, counter, datum_kwargs, validate=True):
    resource_uid = resource['uid']
    any_column, *_ = datum_kwargs.values()
    N = len(any_column)
    doc = {'resource': resource_uid,
           'datum_kwargs': datum_kwargs,
           'datum_id': ['{}/{}'.format(resource_uid, next(counter)) for _ in range(N)]}
    if validate:
        schema_validators[DocumentNames.datum].validate(doc)
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
        schema_validators[DocumentNames.resource].validate(doc)

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
           'num_events': {k: v - 1 for k, v in event_counter.items()}}
    if validate:
        schema_validators[DocumentNames.stop].validate(doc)
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
        schema_validators[DocumentNames.event_page].validate(doc)
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
        schema_validators[DocumentNames.event].validate(doc)
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
        schema_validators[DocumentNames.descriptor].validate(doc)
    if name not in streams:
        streams[name] = set(data_keys)
        event_counter[name] = 1
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
    time : float, optional
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
        schema_validators[DocumentNames.start].validate(doc)

    return ComposeRunBundle(
        doc,
        partial(compose_descriptor, start=doc, streams=streams,
                event_counter=event_counter),
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
    for datum_id, datum_kwargs in itertools.zip_longest(
            datum_page['datum_id'],
            datum_kwarg_list,
            fillvalue={}):
        datum = {'datum_id': datum_id, 'datum_kwargs': datum_kwargs,
                 'resource': resource}
        yield datum


def rechunk_event_pages(event_pages, chunk_size):
    """
    Resizes the event_pages in a iterable of event_pages.

    Parameters
    ----------
    event_pages: Iterabile
        An iterable of event_pages
    chunk_size: integer
        Size of pages to yield

    Yields
    ------
    event_page : dict
    """
    remainder = chunk_size
    chunk_list = []

    def page_chunks(page, chunk_size, remainder):
        """
        Yields chunks of a event_page.
        The first chunk will be of size remainder, the following chunks will be
        of size chunk_size. The last chunk will be what ever is left over.
        """
        array_keys = ['seq_num', 'time', 'uid']
        page_size = len(page['uid'])  # Number of events in the page.

        # Make a list of the chunk indexes.
        chunks = [(0, remainder)]
        chunks.extend([(i, i + chunk_size) for i
                       in range(remainder, page_size, chunk_size)])

        for start, stop in chunks:
            yield {'descriptor': page['descriptor'],
                   **{key: page[key][start:stop] for key in array_keys},
                   'data': {key: page['data'][key][start:stop]
                            for key in page['data'].keys()},
                   'timestamps': {key: page['timestamps'][key][start: stop]
                                  for key in page['timestamps'].keys()},
                   'filled': {key: page['filled'][key][start:stop]
                              for key in page['data'].keys()}}

    for page in event_pages:
        new_chunks = page_chunks(page, chunk_size, remainder)
        for chunk in new_chunks:
            remainder -= len(chunk['uid'])  # Subtract the size of the chunk.
            chunk_list.append(chunk)
            if remainder == 0:
                yield merge_event_pages(chunk_list)
                remainder = chunk_size
                chunk_list = []
    if chunk_list:
        yield merge_event_pages(chunk_list)


def merge_event_pages(event_pages):
    """
    Combines a iterable of event_pages to a single event_page.

    Parameters
    ----------
    event_pages: Iterabile
        An iterable of event_pages

    Returns
    ------
    event_page : dict
    """
    pages = list(event_pages)
    if len(pages) == 1:
        return pages[0]

    array_keys = ['seq_num', 'time', 'uid']

    return {'descriptor': pages[0]['descriptor'],
            **{key: list(itertools.chain.from_iterable(
                    [page[key] for page in pages])) for key in array_keys},
            'data': {key: list(itertools.chain.from_iterable(
                    [page['data'][key] for page in pages]))
                    for key in pages[0]['data'].keys()},
            'timestamps': {key: list(itertools.chain.from_iterable(
                    [page['timestamps'][key] for page in pages]))
                    for key in pages[0]['data'].keys()},
            'filled': {key: list(itertools.chain.from_iterable(
                    [page['filled'][key] for page in pages]))
                    for key in pages[0]['data'].keys()}}


def rechunk_datum_pages(datum_pages, chunk_size):
    """
    Resizes the datum_pages in a iterable of event_pages.

    Parameters
    ----------
    datum_pages: Iterabile
        An iterable of datum_pages
    chunk_size: integer
        Size of pages to yield

    Yields
    ------
    datum_page : dict
    """
    remainder = chunk_size
    chunk_list = []

    def page_chunks(page, chunk_size, remainder):
        """
        Yields chunks of a datum_page.
        The first chunk will be of size remainder, the following chunks will be
        of size chunk_size. The last chunk will be what ever is left over.
        """

        array_keys = ['datum_id']
        page_size = len(page['datum_id'])  # Number of datum in the page.

        # Make a list of the chunk indexes.
        chunks = [(0, remainder)]
        chunks.extend([(i, i + chunk_size) for i
                       in range(remainder, page_size, chunk_size)])

        for start, stop in chunks:
            yield {'resource': page['resource'],
                   **{key: page[key][start:stop] for key in array_keys},
                   'datum_kwargs': {key: page['datum_kwargs'][key][start:stop]
                                    for key in page['datum_kwargs'].keys()}}

    for page in datum_pages:
        new_chunks = page_chunks(page, chunk_size, remainder)
        for chunk in new_chunks:
            remainder -= len(chunk['datum_id'])  # Subtract the size of the chunk.
            chunk_list.append(chunk)
            if remainder == 0:
                yield merge_datum_pages(chunk_list)
                remainder = chunk_size
                chunk_list = []
    if chunk_list:
        yield merge_datum_pages(chunk_list)


def merge_datum_pages(datum_pages):
    """
    Combines a iterable of datum_pages to a single datum_page.

    Parameters
    ----------
    datum_pages: Iterabile
        An iterable of datum_pages

    Returns
    ------
    datum_page : dict
    """
    pages = list(datum_pages)
    if len(pages) == 1:
        return pages[0]

    array_keys = ['datum_id']

    return {'resource': pages[0]['resource'],
            **{key: list(itertools.chain.from_iterable(
                    [page[key] for page in pages])) for key in array_keys},
            'datum_kwargs': {key: list(itertools.chain.from_iterable(
                    [page['datum_kwargs'][key] for page in pages]))
                    for key in pages[0]['datum_kwargs'].keys()}}


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
                page['filled'] = {k: [] for k in event.get('filled', {})}
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
            for k, v in event.get('filled', {}).items():
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
