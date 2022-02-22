"""Document Routers."""

import inspect
import weakref
import warnings
import threading
import os
from collections import defaultdict, deque
import collections
import time as ttime
import copy

from ._repackers import (
    pack_event_page,
    unpack_event_page,
    pack_datum_page,
    unpack_datum_page,
    bulk_datum_to_datum_page,
    bulk_events_to_event_pages,
)
from ._validators import schema_validators
from ._enums import DocumentNames
from ._errors import (
    EventModelError,
    EventModelValueError,
    EventModelKeyError,
    DuplicateHandler,
    EventModelRuntimeError,
    UndefinedAssetSpecification,
    MismatchedDataKeys,
    UnresolvableForeignKeyError,
    DataNotAccessible,
    EventModelTypeError,
    UnfilledData,
)
from ._coercion import _coercion_registry


class HandlerRegistryView(collections.abc.Mapping):
    def __init__(self, handler_registry):
        self._handler_registry = handler_registry

    def __repr__(self):
        return f"HandlerRegistryView({self._handler_registry!r})"

    def __getitem__(self, key):
        return self._handler_registry[key]

    def __iter__(self):
        yield from self._handler_registry

    def __len__(self):
        return len(self._handler_registry)

    def __setitem__(self, key, value):
        raise EventModelTypeError(
            "The handler registry cannot be edited directly. "
            "Instead, use the method Filler.register_handler."
        )

    def __delitem__(self, key):
        raise EventModelTypeError(
            "The handler registry cannot be edited directly. "
            "Instead, use the method Filler.deregister_handler."
        )


DOCS_PASSED_IN_1_14_0_WARNING = (
    "The callback {callback!r} raised {err!r} when "
    "RunRouter passed it a {name!r} document. This is "
    "probably because in earlier releases the RunRouter "
    "expected its factory functions to forward the 'start' "
    "document, but starting in event-model 1.14.0 the "
    "RunRouter passes in the document, causing the "
    "callback to receive it twice and potentially raise "
    "an error. Update the factory function. In a future "
    "release this warning will become an error."
)


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

    Parameters
    ----------
    emit: callable, optional
        Expected signature ``f(name, doc)``
    """

    def __init__(self, *, emit=None):
        # Put in some extra effort to validate `emit` carefully, because if
        # this is used incorrectly the resultant errors can be confusing.
        if emit is not None:
            if not callable(emit):
                raise ValueError("emit must be a callable")
            sig = inspect.signature(emit)
            try:
                # Does this function accept two positional arguments?
                sig.bind(None, None)
            except TypeError:
                raise ValueError(
                    "emit must accept two positional arguments, name and doc"
                )
            # Stash a weak reference to `emit`.
            if inspect.ismethod(emit):
                self._emit_ref = weakref.WeakMethod(emit)
            else:
                self._emit_ref = weakref.ref(emit)
        else:
            self._emit_ref = None

    def emit(self, name, doc):
        """Emit to the callable provided an instantiation time, if any."""
        if self._emit_ref is not None:
            # Call the weakref.
            emit = self._emit_ref()
            if emit is not None:
                emit(name, doc)

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
        return self._dispatch(name, doc, validate)

    def _dispatch(self, name, doc, validate):
        """
        Dispatch to the method corresponding to the `name`.

        Optionally validate that the result is still a valid document.
        """
        output_doc = getattr(self, name)(doc)

        # If 'event' is not defined by the subclass but 'event_page' is, or
        # vice versa, use that. And the same for 'datum_page' / 'datum.
        if output_doc is NotImplemented:
            if name == "event":
                event_page = pack_event_page(doc)
                # Subclass' implementation of event_page may return a valid
                # EventPage or None or NotImplemented.
                output_event_page = self.event_page(event_page)
                output_event_page = (
                    output_event_page if output_event_page is not None else event_page
                )
                if output_event_page is not NotImplemented:
                    (output_doc,) = unpack_event_page(output_event_page)
            elif name == "datum":
                datum_page = pack_datum_page(doc)
                # Subclass' implementation of datum_page may return a valid
                # DatumPage or None or NotImplemented.
                output_datum_page = self.datum_page(datum_page)
                output_datum_page = (
                    output_datum_page if output_datum_page is not None else datum_page
                )
                if output_datum_page is not NotImplemented:
                    (output_doc,) = unpack_datum_page(output_datum_page)
            elif name == "event_page":
                output_events = []
                for event in unpack_event_page(doc):
                    # Subclass' implementation of event may return a valid
                    # Event or None or NotImplemented.
                    output_event = self.event(event)
                    output_event = output_event if output_event is not None else event
                    if output_event is NotImplemented:
                        break
                    output_events.append(output_event)
                else:
                    output_doc = pack_event_page(*output_events)
            elif name == "datum_page":
                output_datums = []
                for datum in unpack_datum_page(doc):
                    # Subclass' implementation of datum may return a valid
                    # Datum or None or NotImplemented.
                    output_datum = self.datum(datum)
                    output_datum = output_datum if output_datum is not None else datum
                    if output_datum is NotImplemented:
                        break
                    output_datums.append(output_datum)
                else:
                    output_doc = pack_datum_page(*output_datums)
        # If we still don't find an implemented method by here, then pass the
        # original document through.
        if output_doc is NotImplemented:
            output_doc = doc
        if validate:
            schema_validators[getattr(DocumentNames, name)].validate(output_doc)
        return (name, output_doc if output_doc is not None else doc)

    # The methods below return NotImplemented, a built-in Python constant.
    # Note that it is not interchangeable with NotImplementedError. See docs at
    # https://docs.python.org/3/library/constants.html#NotImplemented
    # It is used here so that _dispatch, defined above, can detect whether a
    # subclass implements event, event_page, both, or neither. This is similar
    # to how Python uses NotImplemented in arithmetic operations, as described
    # in the documentation.

    def start(self, doc):
        return NotImplemented

    def stop(self, doc):
        return NotImplemented

    def descriptor(self, doc):
        return NotImplemented

    def resource(self, doc):
        return NotImplemented

    def event(self, doc):
        return NotImplemented

    def datum(self, doc):
        return NotImplemented

    def event_page(self, doc):
        return NotImplemented

    def datum_page(self, doc):
        return NotImplemented

    def bulk_events(self, doc):
        # Do not modify this in a subclass. Use event_page.
        warnings.warn(
            "The document type 'bulk_events' has been deprecated in favor of "
            "'event_page', whose structure is a transpose of 'bulk_events'."
        )
        for page in bulk_events_to_event_pages(doc):
            self.event_page(page)

    def bulk_datum(self, doc):
        # Do not modify this in a subclass. Use event_page.
        warnings.warn(
            "The document type 'bulk_datum' has been deprecated in favor of "
            "'datum_page', whose structure is a transpose of 'bulk_datum'."
        )
        self.datum_page(bulk_datum_to_datum_page(doc))


class SingleRunDocumentRouter(DocumentRouter):
    """
    A DocumentRouter intended to process events from exactly one run.
    """

    def __init__(self):
        super().__init__()
        self._start_doc = None
        self._descriptors = dict()

    def __call__(self, name, doc, validate=False):
        """
        Process a document.

        Also, track of the start document and descriptor documents
        passed to this SingleRunDocumentRouter in caches.

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
        if name == "start":
            if self._start_doc is None:
                self._start_doc = doc
            else:
                raise EventModelValueError(
                    f'SingleRunDocumentRouter associated with start document {self._start_doc["uid"]} '
                    f'received a second start document with uid {doc["uid"]}'
                )
        elif name == "descriptor":
            if doc["run_start"] == self._start_doc["uid"]:
                self._descriptors[doc["uid"]] = doc
            else:
                raise EventModelValueError(
                    f'SingleRunDocumentRouter associated with start document {self._start_doc["uid"]} '
                    f'received a descriptor {doc["uid"]} associated with start document {doc["run_start"]}'
                )
        # Defer to superclass for dispatch/processing.
        return super().__call__(name, doc, validate)

    def get_start(self):
        """Convenience method returning the start document for the associated run.

        If no start document has been processed EventModelError will be raised.

        Returns
        -------
        start document : dict
        """
        if self._start_doc is None:
            raise EventModelError(
                "SingleRunDocumentRouter has not processed a start document yet"
            )

        return self._start_doc

    def get_descriptor(self, doc):
        """Convenience method returning the descriptor associated with the specified document.

        Parameters
        ----------
        doc : dict
            event-model document

        Returns
        -------
        descriptor document : dict
        """
        if "descriptor" not in doc:
            raise EventModelValueError(
                f"document is not associated with a descriptor:\n{doc}"
            )
        elif doc["descriptor"] not in self._descriptors:
            raise EventModelValueError(
                f'SingleRunDocumentRouter has not processed a descriptor with uid {doc["descriptor"]}'
            )

        return self._descriptors[doc["descriptor"]]

    def get_stream_name(self, doc):
        """Convenience method returning the name of the stream for the specified document.

        Parameters
        ----------
        doc : dict
            event-model document

        Returns
        -------
        stream name : str
        """
        return self.get_descriptor(doc).get("name")


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

            handler_class(full_path, **resource_kwargs)

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
    coerce : {'as_is', 'numpy'}
        Default is 'as_is'. Other options (e.g. 'delayed') may be registered by
        external packages at runtime.
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

    def __init__(
        self,
        handler_registry,
        *,
        include=None,
        exclude=None,
        root_map=None,
        coerce="as_is",
        handler_cache=None,
        resource_cache=None,
        datum_cache=None,
        descriptor_cache=None,
        inplace=None,
        retry_intervals=(
            0.001,
            0.002,
            0.004,
            0.008,
            0.016,
            0.032,
            0.064,
            0.128,
            0.256,
            0.512,
            1.024,
        ),
    ):
        if inplace is None:
            self._inplace = True
            warnings.warn(
                "'inplace' argument not specified. It is recommended to "
                "specify True or False. In future releases, 'inplace' "
                "will default to False."
            )
        else:
            self._inplace = inplace

        if include is not None and exclude is not None:
            raise EventModelValueError(
                "The parameters `include` and `exclude` are mutually "
                "incompatible. At least one must be left as the default, "
                "None."
            )
        try:
            self._coercion_func = _coercion_registry[coerce]
        except KeyError:
            raise EventModelKeyError(
                f"The option coerce={coerce!r} was given to event_model.Filler. "
                f"The valid options are {set(_coercion_registry)}."
            )
        self._coerce = coerce

        # See comments on coerision functions above for the use of
        # _current_state, which is passed to coercion functions' `filler_state`
        # parameter.
        self._current_state = threading.local()
        self._unpatched_handler_registry = {}
        self._handler_registry = {}
        for spec, handler_class in handler_registry.items():
            self.register_handler(spec, handler_class)
        self.handler_registry = HandlerRegistryView(self._handler_registry)
        if include is not None:
            warnings.warn(
                "In a future release of event-model, the argument `include` "
                "will be removed from Filler.",
                DeprecationWarning,
            )
        self.include = include
        if exclude is not None:
            warnings.warn(
                "In a future release of event-model, the argument `exclude` "
                "will be removed from Filler.",
                DeprecationWarning,
            )
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
        self.retry_intervals = retry_intervals
        self._closed = False

    def __eq__(self, other):
        return (
            type(self) is type(other)
            and self.inplace == other.inplace
            and self._coerce == other._coerce
            and self.include == other.include
            and self.exclude == other.exclude
            and self.root_map == other.root_map
            and type(self._handler_cache) is type(other._handler_cache)
            and type(self._resource_cache) is type(other._resource_cache)
            and type(self._datum_cache) is type(other._datum_cache)
            and type(self._descriptor_cache) is type(other._descriptor_cache)
            and self.retry_intervals == other.retry_intervals
        )

    def __getstate__(self):
        return dict(
            inplace=self._inplace,
            coercion_func=self._coerce,
            handler_registry=self._unpatched_handler_registry,
            include=self.include,
            exclude=self.exclude,
            root_map=self.root_map,
            handler_cache=self._handler_cache,
            resource_cache=self._resource_cache,
            datum_cache=self._datum_cache,
            descriptor_cache=self._descriptor_cache,
            retry_intervals=self.retry_intervals,
        )

    def __setstate__(self, d):
        self._inplace = d["inplace"]
        self._coerce = d["coercion_func"]

        # See comments on coerision functions above for the use of
        # _current_state, which is passed to coercion functions' `filler_state`
        # parameter.
        self._current_state = threading.local()
        self._unpatched_handler_registry = {}
        self._handler_registry = {}
        for spec, handler_class in d["handler_registry"].items():
            self.register_handler(spec, handler_class)
        self.handler_registry = HandlerRegistryView(self._handler_registry)
        self.include = d["include"]
        self.exclude = d["exclude"]
        self.root_map = d["root_map"]
        self._handler_cache = d["handler_cache"]
        self._resource_cache = d["resource_cache"]
        self._datum_cache = d["datum_cache"]
        self._descriptor_cache = d["descriptor_cache"]
        retry_intervals = d["retry_intervals"]
        if retry_intervals is None:
            retry_intervals = []
        self._retry_intervals = retry_intervals
        self._closed = False

    @property
    def retry_intervals(self):
        return self._retry_intervals

    @retry_intervals.setter
    def retry_intervals(self, value):
        self._retry_intervals = list(value)

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

    def clone(
        self,
        handler_registry=None,
        *,
        root_map=None,
        coerce=None,
        handler_cache=None,
        resource_cache=None,
        datum_cache=None,
        descriptor_cache=None,
        inplace=None,
        retry_intervals=None,
    ):
        """
        Create a new Filler instance from this one.

        By default it will be created with the same settings that this Filler
        has. Individual settings may be overridden here.

        The clone does *not* share any caches or internal state with the
        original.
        """
        if handler_registry is None:
            handler_registry = self._unpatched_handler_registry
        if root_map is None:
            root_map = self.root_map
        if coerce is None:
            coerce = self._coerce
        if inplace is None:
            inplace = self.inplace
        if retry_intervals is None:
            retry_intervals = self.retry_intervals
        return Filler(
            handler_registry,
            root_map=root_map,
            coerce=coerce,
            handler_cache=handler_cache,
            resource_cache=resource_cache,
            datum_cache=datum_cache,
            descriptor_cache=descriptor_cache,
            inplace=inplace,
            retry_intervals=retry_intervals,
        )

    def register_handler(self, spec, handler, overwrite=False):
        """
        Register a handler.

        Parameters
        ----------
        spec: str
        handler: Handler
        overwrite: boolean, optional
            False by default

        Raises
        ------
        DuplicateHandler
            If a handler is already registered for spec and overwrite is False

        See https://blueskyproject.io/event-model/external.html
        """
        if (not overwrite) and (spec in self._handler_registry):
            original = self._unpatched_handler_registry[spec]
            if original is handler:
                return
            raise DuplicateHandler(
                f"There is already a handler registered for the spec {spec!r}. "
                f"Use overwrite=True to deregister the original.\n"
                f"Original: {original}\n"
                f"New: {handler}"
            )

        self.deregister_handler(spec)
        # Keep a raw copy, unused above for identifying redundant registration.
        self._unpatched_handler_registry[spec] = handler
        # Let the 'coerce' argument to Filler.__init__ modify the handler if it
        # wants to.
        self._handler_registry[spec] = self._coercion_func(handler, self._current_state)

    def deregister_handler(self, spec):
        """
        Deregister a handler.

        If no handler is registered for this spec, it is no-op and returns
        None.

        Parameters
        ----------
        spec: str

        Returns
        -------
        handler: Handler or None

        See https://blueskyproject.io/event-model/external.html
        """
        handler = self._handler_registry.pop(spec, None)
        if handler is not None:
            self._unpatched_handler_registry.pop(spec)
            for key in list(self._handler_cache):
                resource_uid, spec_ = key
                if spec == spec_:
                    del self._handler_cache[key]
        return handler

    def resource(self, doc):
        # Defer creating the handler instance until we actually need it, when
        # we fill the first Event field that requires this Resource.
        self._resource_cache[doc["uid"]] = doc
        return doc

    # Handlers operate document-wise, so we'll explode pages into individual
    # documents.

    def datum_page(self, doc):
        datum = self.datum  # Avoid attribute lookup in hot loop.
        for datum_doc in unpack_datum_page(doc):
            datum(datum_doc)
        return doc

    def datum(self, doc):
        self._datum_cache[doc["datum_id"]] = doc
        return doc

    def event_page(self, doc):
        # TODO We may be able to fill a page in place, and that may be more
        # efficient than unpacking the page in to Events, filling them, and the
        # re-packing a new page. But that seems tricky in general since the
        # page may be implemented as a DataFrame or dict, etc.
        filled_doc = self.fill_event_page(
            doc, include=self.include, exclude=self.exclude
        )
        return filled_doc

    def event(self, doc):
        filled_doc = self.fill_event(doc, include=self.include, exclude=self.exclude)
        return filled_doc

    def fill_event_page(self, doc, include=None, exclude=None, inplace=None):
        filled_events = []
        for event_doc in unpack_event_page(doc):
            filled_events.append(
                self.fill_event(
                    event_doc, include=include, exclude=exclude, inplace=True
                )
            )
        filled_doc = pack_event_page(*filled_events)
        if inplace is None:
            inplace = self._inplace
        if inplace:
            doc["data"] = filled_doc["data"]
            doc["filled"] = filled_doc["filled"]
            return doc
        else:
            return filled_doc

    def get_handler(self, resource):
        """
        Return a new Handler instance for this Resource.

        Parameters
        ----------
        resource: dict

        Returns
        -------
        handler: Handler
        """
        if self._closed:
            raise EventModelRuntimeError(
                "This Filler has been closed and is no longer usable."
            )
        try:
            handler_class = self.handler_registry[resource["spec"]]
        except KeyError as err:
            raise UndefinedAssetSpecification(
                f"Resource document with uid {resource['uid']} "
                f"refers to spec {resource['spec']!r} which is "
                f"not defined in the Filler's "
                f"handler registry."
            ) from err
        # Apply root_map.
        resource_path = resource["resource_path"]
        original_root = resource.get("root", "")
        root = self.root_map.get(original_root, original_root)
        if root:
            resource_path = os.path.join(root, resource_path)
        msg = (
            f"Error instantiating handler "
            f"class {handler_class} "
            f"with Resource document {resource}. "
        )
        if root != original_root:
            msg += (
                f"Its 'root' field was "
                f"mapped from {original_root} to {root} by root_map."
            )
        else:
            msg += (
                f"Its 'root' field {original_root} was " f"*not* modified by root_map."
            )
        error_to_raise = EventModelError(msg)
        handler = _attempt_with_retries(
            func=handler_class,
            args=(resource_path,),
            kwargs=resource["resource_kwargs"],
            intervals=[0] + self.retry_intervals,
            error_to_catch=IOError,
            error_to_raise=error_to_raise,
        )
        return handler

    def _get_handler_maybe_cached(self, resource):
        "Get a cached handler for this resource or make one and cache it."
        key = (resource["uid"], resource["spec"])
        try:
            handler = self._handler_cache[key]
        except KeyError:
            handler = self.get_handler(resource)
            self._handler_cache[key] = handler
        return handler

    def fill_event(self, doc, include=None, exclude=None, inplace=None):
        if inplace is None:
            inplace = self._inplace
        if inplace:
            filled_doc = doc
        else:
            filled_doc = copy.deepcopy(doc)
        descriptor = self._descriptor_cache[doc["descriptor"]]
        from_datakeys = False
        self._current_state.descriptor = descriptor
        try:
            needs_filling = {key for key, val in doc["filled"].items() if val is False}
        except KeyError:
            # This document is not telling us which, if any, keys are filled.
            # Infer that none of the external data is filled.
            needs_filling = {
                key for key, val in descriptor["data_keys"].items() if "external" in val
            }
            from_datakeys = True
        for key in needs_filling:
            self._current_state.key = key
            if exclude is not None and key in exclude:
                continue
            if include is not None and key not in include:
                continue
            try:
                datum_id = doc["data"][key]
            except KeyError as err:
                if from_datakeys:
                    raise MismatchedDataKeys(
                        "The documents are not valid.  Either because they "
                        "were recorded incorrectly in the first place, "
                        "corrupted since, or exercising a yet-undiscovered "
                        "bug in a reader. event['data'].keys() "
                        "must equal descriptor['data_keys'].keys(). "
                        f"event['data'].keys(): {doc['data'].keys()}, "
                        "descriptor['data_keys'].keys(): "
                        f"{descriptor['data_keys'].keys()}"
                    ) from err
                else:
                    raise MismatchedDataKeys(
                        "The documents are not valid.  Either because they "
                        "were recorded incorrectly in the first place, "
                        "corrupted since, or exercising a yet-undiscovered "
                        "bug in a reader. event['filled'].keys() "
                        "must be a subset of event['data'].keys(). "
                        f"event['data'].keys(): {doc['data'].keys()}, "
                        "event['filled'].keys(): "
                        f"{doc['filled'].keys()}"
                    ) from err
            # Look up the cached Datum doc.
            try:
                datum_doc = self._datum_cache[datum_id]
            except KeyError as err:
                raise UnresolvableForeignKeyError(
                    datum_id,
                    f"Event with uid {doc['uid']} refers to unknown Datum "
                    f"datum_id {datum_id}",
                ) from err
            resource_uid = datum_doc["resource"]
            # Look up the cached Resource.
            try:
                resource = self._resource_cache[resource_uid]
            except KeyError as err:
                raise UnresolvableForeignKeyError(
                    resource_uid,
                    f"Datum with id {datum_id} refers to unknown Resource "
                    f"uid {resource_uid}",
                ) from err
            self._current_state.resource = resource
            self._current_state.datum = datum_doc
            handler = self._get_handler_maybe_cached(resource)
            error_to_raise = DataNotAccessible(
                f"Filler was unable to load the data referenced by "
                f"the Datum document {datum_doc} and the Resource "
                f"document {resource}."
            )
            payload = _attempt_with_retries(
                func=handler,
                args=(),
                kwargs=datum_doc["datum_kwargs"],
                intervals=[0] + self.retry_intervals,
                error_to_catch=IOError,
                error_to_raise=error_to_raise,
            )
            # Here we are intentionally modifying doc in place.
            filled_doc["data"][key] = payload
            filled_doc["filled"][key] = datum_id
        self._current_state.key = None
        self._current_state.descriptor = None
        self._current_state.resource = None
        self._current_state.datum = None
        return filled_doc

    def descriptor(self, doc):
        self._descriptor_cache[doc["uid"]] = doc
        return doc

    def __enter__(self):
        return self

    def close(self):
        """
        Drop cached documents and handlers.

        They are *not* explicitly cleared, so if there are other references to
        these caches they will remain.
        """
        # Drop references to the caches. If the user holds another reference to
        # them it's the user's problem to manage their lifecycle. If the user
        # does not (e.g. they are the default caches) the gc will look after
        # them.
        self._closed = True
        self._handler_cache = None
        self._resource_cache = None
        self._datum_cache = None
        self._descriptor_cache = None

    @property
    def closed(self):
        return self._closed

    def clear_handler_cache(self):
        """
        Clear any cached handler instances.

        This operation may free significant memory, depending on the
        implementation of the handlers.
        """
        self._handler_cache.clear()

    def clear_document_caches(self):
        """
        Clear any cached documents.
        """
        self._resource_cache.clear()
        self._descriptor_cache.clear()
        self._datum_cache.clear()

    def __exit__(self, *exc_details):
        self.close()

    def __call__(self, name, doc, validate=False):
        if self._closed:
            raise EventModelRuntimeError(
                "This Filler has been closed and is no longer usable."
            )
        return super().__call__(name, doc, validate)


class NoFiller(Filler):
    """
    This does not fill the documents; it merely validates them.

    It checks that all the references between the documents are resolvable and
    *could* be filled. This is useful when the filling will be done later, as
    a delayed computation, but we want to make sure in advance that we have all
    the information that we will need when that computation occurs.
    """

    def __init__(self, *args, **kwargs):
        # Do not make Filler make copies because we are not going to alter the
        # documents anyway.
        kwargs.setdefault("inplace", True)
        super().__init__(*args, **kwargs)

    def fill_event_page(self, doc, include=None, exclude=None):
        filled_events = []
        for event_doc in unpack_event_page(doc):
            filled_events.append(
                self.fill_event(
                    event_doc, include=include, exclude=exclude, inplace=True
                )
            )
        filled_doc = pack_event_page(*filled_events)
        return filled_doc

    def fill_event(self, doc, include=None, exclude=None, inplace=None):
        descriptor = self._descriptor_cache[doc["descriptor"]]
        from_datakeys = False
        try:
            needs_filling = {key for key, val in doc["filled"].items() if val is False}
        except KeyError:
            # This document is not telling us which, if any, keys are filled.
            # Infer that none of the external data is filled.
            needs_filling = {
                key for key, val in descriptor["data_keys"].items() if "external" in val
            }
            from_datakeys = True
        for key in needs_filling:
            if exclude is not None and key in exclude:
                continue
            if include is not None and key not in include:
                continue
            try:
                datum_id = doc["data"][key]
            except KeyError as err:
                if from_datakeys:
                    raise MismatchedDataKeys(
                        "The documents are not valid.  Either because they "
                        "were recorded incorrectly in the first place, "
                        "corrupted since, or exercising a yet-undiscovered "
                        "bug in a reader. event['data'].keys() "
                        "must equal descriptor['data_keys'].keys(). "
                        f"event['data'].keys(): {doc['data'].keys()}, "
                        "descriptor['data_keys'].keys(): "
                        f"{descriptor['data_keys'].keys()}"
                    ) from err
                else:
                    raise MismatchedDataKeys(
                        "The documents are not valid.  Either because they "
                        "were recorded incorrectly in the first place, "
                        "corrupted since, or exercising a yet-undiscovered "
                        "bug in a reader. event['filled'].keys() "
                        "must be a subset of event['data'].keys(). "
                        f"event['data'].keys(): {doc['data'].keys()}, "
                        "event['filled'].keys(): "
                        f"{doc['filled'].keys()}"
                    ) from err
            # Look up the cached Datum doc.
            try:
                datum_doc = self._datum_cache[datum_id]
            except KeyError as err:
                err_with_key = UnresolvableForeignKeyError(
                    datum_id,
                    f"Event with uid {doc['uid']} refers to unknown Datum "
                    f"datum_id {datum_id}",
                )
                err_with_key.key = datum_id
                raise err_with_key from err
            resource_uid = datum_doc["resource"]
            # Look up the cached Resource.
            try:
                self._resource_cache[resource_uid]
            except KeyError as err:
                raise UnresolvableForeignKeyError(
                    datum_id,
                    f"Datum with id {datum_id} refers to unknown Resource "
                    f"uid {resource_uid}",
                ) from err
        return doc


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

        that will receive that RunStart document and all subsequent documents
        from the run including the RunStop document. All items in the second
        list should be "subfactories" with the signature::

            subfactory('descriptor', descriptor_doc) -> List[Callbacks]

        These will receive each of the EventDescriptor documents for the run,
        as they arrive. They must return one list, which may be empty,
        containing callbacks that will receive the RunStart document, that
        EventDescriptor, all Events that reference that EventDescriptor and
        finally the RunStop document for the run.
    handler_registry : dict, optional
        This is passed to the Filler or whatever class is given in the
        filler_class parametr below.

        Maps each 'spec' (a string identifying a given type or external
        resource) to a handler class.

        A 'handler class' may be any callable with the signature::

            handler_class(full_path, **resource_kwargs)

        It is expected to return an object, a 'handler instance', which is also
        callable and has the following signature::

            handler_instance(**datum_kwargs)

        As the names 'handler class' and 'handler instance' suggest, this is
        typically implemented using a class that implements ``__init__`` and
        ``__call__``, with the respective signatures. But in general it may be
        any callable-that-returns-a-callable.
    root_map: dict, optional
        This is passed to Filler or whatever class is given in the filler_class
        parameter below.

        str -> str mapping to account for temporarily moved/copied/remounted
        files.  Any resources which have a ``root`` in ``root_map`` will be
        loaded using the mapped ``root``.
    filler_class: type
        This is Filler by default. It can be a Filler subclass,
        ``functools.partial(Filler, ...)``, or any class that provides the same
        methods as ``DocumentRouter``.
    fill_or_fail: boolean, optional
        By default (False), if a document with a spec not in
        ``handler_registry`` is encountered, let it pass through unfilled. But
        if set to True, fill everything and `raise
        ``UndefinedAssetSpecification`` if some unknown spec is encountered.
    """

    def __init__(
        self,
        factories,
        handler_registry=None,
        *,
        root_map=None,
        filler_class=Filler,
        fill_or_fail=False,
    ):
        self.factories = factories
        self.handler_registry = handler_registry or {}
        self.filler_class = filler_class
        self.fill_or_fail = fill_or_fail
        self.root_map = root_map

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

        # Map RunStart UID to RunStart document. This is used to send
        # RunStart documents to subfactory callbacks.
        self._start_to_start_doc = dict()

        # Map RunStart UID to the list EventDescriptor. This is used to
        # facilitate efficient cleanup of the caches above.
        self._start_to_descriptors = defaultdict(list)

        # Map EventDescriptor UID to RunStart UID. This is used for looking up
        # Fillers.
        self._descriptor_to_start = {}

        # Map Resource UID to RunStart UID.
        self._resources = {}

        # Old-style Resources that do not have a RunStart UID
        self._unlabeled_resources = deque(maxlen=10000)

        # Map Runstart UID to instances of self.filler_class.
        self._fillers = {}

    def __repr__(self):
        return (
            "RunRouter([\n"
            + "\n".join(f"    {factory}" for factory in self.factories)
            + "])"
        )

    def start(self, start_doc):
        uid = start_doc["uid"]
        # If we get the same uid twice, weird things will happen, so check for
        # that and give a nice error message.
        if uid in self._start_to_start_doc:
            if self._start_to_start_doc[uid] == start_doc:
                raise ValueError(
                    "RunRouter received the same 'start' document twice:\n"
                    "{start_doc!r}"
                )
            else:
                raise ValueError(
                    "RunRouter received two 'start' documents with different "
                    "contents but the same uid:\n"
                    "First: {self._start_to_start_doc[uid]!r}\n"
                    "Second: {start_doc!r}"
                )
        self._start_to_start_doc[uid] = start_doc
        filler = self.filler_class(
            self.handler_registry, root_map=self.root_map, inplace=False
        )
        self._fillers[uid] = filler
        # No need to pass the document to filler
        # because Fillers do nothing with 'start'.
        for factory in self.factories:
            callbacks, subfactories = factory("start", start_doc)
            for callback in callbacks:
                try:
                    callback("start", start_doc)
                except Exception as err:
                    warnings.warn(
                        DOCS_PASSED_IN_1_14_0_WARNING.format(
                            callback=callback, name="start", err=err
                        )
                    )
                    raise err
            self._factory_cbs_by_start[uid].extend(callbacks)
            self._subfactories[uid].extend(subfactories)

    def descriptor(self, descriptor_doc):
        descriptor_uid = descriptor_doc["uid"]
        start_uid = descriptor_doc["run_start"]

        # Keep track of the RunStart UID -> [EventDescriptor UIDs] mapping for
        # purposes of cleanup in stop().
        self._start_to_descriptors[start_uid].append(descriptor_uid)
        # Keep track of the EventDescriptor UID -> RunStartUID for filling
        # purposes.
        self._descriptor_to_start[descriptor_uid] = start_uid

        self._fillers[start_uid].descriptor(descriptor_doc)
        # Apply all factory cbs for this run to this descriptor, and run them.
        factory_cbs = self._factory_cbs_by_start[start_uid]
        self._factory_cbs_by_descriptor[descriptor_uid].extend(factory_cbs)
        for callback in factory_cbs:
            callback("descriptor", descriptor_doc)
        # Let all the subfactories add any relevant callbacks.
        for subfactory in self._subfactories[start_uid]:
            callbacks = subfactory("descriptor", descriptor_doc)
            self._subfactory_cbs_by_start[start_uid].extend(callbacks)
            self._subfactory_cbs_by_descriptor[descriptor_uid].extend(callbacks)
            for callback in callbacks:
                try:
                    start_doc = self._start_to_start_doc[start_uid]
                    callback("start", start_doc)
                except Exception as err:
                    warnings.warn(
                        DOCS_PASSED_IN_1_14_0_WARNING.format(
                            callback=callback, name="start", err=err
                        )
                    )
                    raise err
                try:
                    callback("descriptor", descriptor_doc)
                except Exception as err:
                    warnings.warn(
                        DOCS_PASSED_IN_1_14_0_WARNING.format(
                            callback=callback, name="descriptor", err=err
                        )
                    )
                    raise err

    def event_page(self, doc):
        descriptor_uid = doc["descriptor"]
        start_uid = self._descriptor_to_start[descriptor_uid]
        try:
            doc = self._fillers[start_uid].event_page(doc)
        except UndefinedAssetSpecification:
            if self.fill_or_fail:
                raise
        for callback in self._factory_cbs_by_descriptor[descriptor_uid]:
            callback("event_page", doc)
        for callback in self._subfactory_cbs_by_descriptor[descriptor_uid]:
            callback("event_page", doc)

    def datum_page(self, doc):
        resource_uid = doc["resource"]
        try:
            start_uid = self._resources[resource_uid]
        except KeyError:
            if resource_uid not in self._unlabeled_resources:
                raise UnresolvableForeignKeyError(
                    resource_uid,
                    f"DatumPage refers to unknown Resource uid {resource_uid}",
                )
            # Old Resources do not have a reference to a RunStart document,
            # so in turn we cannot immediately tell which run these datum
            # documents belong to.
            # Fan them out to every run currently flowing through RunRouter. If
            # they are not applicable they will do no harm, and this is
            # expected to be an increasingly rare case.
            for callbacks in self._factory_cbs_by_start.values():
                for callback in callbacks:
                    callback("datum_page", doc)
            for callbacks in self._subfactory_cbs_by_start.values():
                for callback in callbacks:
                    callback("datum_page", doc)
            for filler in self._fillers.values():
                filler.datum_page(doc)
        else:
            self._fillers[start_uid].datum_page(doc)
            for callback in self._factory_cbs_by_start[start_uid]:
                callback("datum_page", doc)
            for callback in self._subfactory_cbs_by_start[start_uid]:
                callback("datum_page", doc)

    def resource(self, doc):
        try:
            start_uid = doc["run_start"]
        except KeyError:
            # Old Resources do not have a reference to a RunStart document.
            # Fan them out to every run currently flowing through RunRouter. If
            # they are not applicable they will do no harm, and this is
            # expected to be an increasingly rare case.
            self._unlabeled_resources.append(doc["uid"])
            for callbacks in self._factory_cbs_by_start.values():
                for callback in callbacks:
                    callback("resource", doc)
            for callbacks in self._subfactory_cbs_by_start.values():
                for callback in callbacks:
                    callback("resource", doc)
            for filler in self._fillers.values():
                filler.resource(doc)
        else:
            self._fillers[start_uid].resource(doc)
            self._resources[doc["uid"]] = doc["run_start"]
            for callback in self._factory_cbs_by_start[start_uid]:
                callback("resource", doc)
            for callback in self._subfactory_cbs_by_start[start_uid]:
                callback("resource", doc)

    def stop(self, doc):
        start_uid = doc["run_start"]
        for callback in self._factory_cbs_by_start[start_uid]:
            callback("stop", doc)
        for callback in self._subfactory_cbs_by_start[start_uid]:
            callback("stop", doc)
        # Clean up references.
        self._fillers.pop(start_uid, None)
        self._subfactories.pop(start_uid, None)
        self._factory_cbs_by_start.pop(start_uid, None)
        self._subfactory_cbs_by_start.pop(start_uid, None)
        for descriptor_uid in self._start_to_descriptors.pop(start_uid, ()):
            self._descriptor_to_start.pop(descriptor_uid, None)
            self._factory_cbs_by_descriptor.pop(descriptor_uid, None)
            self._subfactory_cbs_by_descriptor.pop(descriptor_uid, None)
        self._resources.pop(start_uid, None)
        self._start_to_start_doc.pop(start_uid, None)


def _attempt_with_retries(
    func, args, kwargs, intervals, error_to_catch, error_to_raise
):
    """
    Return func(*args, **kwargs), using a retry loop.

    func, args, kwargs: self-explanatory
    intervals: list
        How long to wait (seconds) between each attempt including the first.
    error_to_catch: Exception class
        If this is raised, retry.
    error_to_raise: Exception instance or class
        If we run out of retries, raise this from the proximate error.
    """
    error = None
    for interval in intervals:
        ttime.sleep(interval)
        try:
            return func(*args, **kwargs)
        except error_to_catch as error_:
            # The file may not be visible on the filesystem yet.
            # Wait and try again. Stash the error in a variable
            # that we can access later if we run out of attempts.
            error = error_
        else:
            break
    else:
        # We have used up all our attempts. There seems to be an
        # actual problem. Raise specified error from the error stashed above.
        raise error_to_raise from error


def verify_filled(event_page):
    """Take an event_page document and verify that it is completely filled.

    Parameters
    ----------
    event_page : event_page document
        The event page document to check

    Raises
    ------
    UnfilledData
        Raised if any of the data in the event_page is unfilled, when raised it
        inlcudes a list of unfilled data objects in the exception message.
    """
    if not all(map(all, event_page["filled"].values())):
        # check that all event_page data is filled.
        unfilled_data = []
        for field, filled in event_page["filled"].items():
            if not all(filled):
                unfilled_data.append(field)
                raise UnfilledData(
                    f"Unfilled data found in fields "
                    f"{unfilled_data!r}. Use "
                    f"`event_model.Filler`."
                )
