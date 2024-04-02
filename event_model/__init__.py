import collections.abc
import copy
import inspect
import itertools
import json
import os
import sys
import threading
import time as ttime
import uuid
import warnings
import weakref
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum
from importlib.metadata import version as importlib_version
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    no_type_check,
)

import jsonschema
import numpy
from typing_extensions import Literal

from .documents.datum import Datum
from .documents.datum_page import DatumPage
from .documents.event import Event
from .documents.event_descriptor import (
    Configuration,
    DataKey,
    EventDescriptor,
    PerObjectHint,
)
from .documents.event_page import EventPage
from .documents.resource import Resource
from .documents.run_start import RunStart
from .documents.run_stop import RunStop
from .documents.stream_datum import StreamDatum, StreamRange
from .documents.stream_resource import StreamResource

if sys.version_info < (3, 9):
    import importlib_resources
else:
    import importlib.resources as importlib_resources

__version__ = importlib_version("event-model")

del importlib_version


__all__ = [
    "DocumentNames",
    "schemas",
    "schema_validators",
    "compose_run",
    "__version__",
]


class DocumentNames(Enum):
    stop = "stop"
    start = "start"
    descriptor = "descriptor"
    event = "event"
    datum = "datum"
    resource = "resource"
    event_page = "event_page"
    datum_page = "datum_page"
    stream_resource = "stream_resource"
    stream_datum = "stream_datum"
    bulk_datum = "bulk_datum"  # deprecated
    bulk_events = "bulk_events"  # deprecated


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

    def __init__(self, *, emit: Optional[Callable] = None) -> None:
        # Put in some extra effort to validate `emit` carefully, because if
        # this is used incorrectly the resultant errors can be confusing.

        self._emit_ref: Optional[Callable] = None

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

    def emit(self, name: str, doc: dict) -> None:
        """
        Emit to the callable provided an instantiation time, if any.
        """
        if self._emit_ref is not None:
            # Call the weakref.
            emit = self._emit_ref()
            if emit is not None:
                emit(name, doc)

    def __call__(
        self, name: str, doc: dict, validate: bool = False
    ) -> Tuple[str, dict]:
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

    def _dispatch(self, name: str, doc: dict, validate: bool) -> Tuple[str, dict]:
        """
        Dispatch to the method corresponding to the `name`.

        Optionally validate that the result is still a valid document.
        """
        output_doc = getattr(self, name)(doc)

        # If 'event' is not defined by the subclass but 'event_page' is, or
        # vice versa, use that. And the same for 'datum_page' / 'datum.
        if output_doc is NotImplemented:
            if name == "event":
                event_page = pack_event_page(cast(Event, doc))
                # Subclass' implementation of event_page may return a valid
                # EventPage or None or NotImplemented.
                output_event_page = self.event_page(event_page)
                output_event_page = (
                    output_event_page if output_event_page is not None else event_page
                )
                if output_event_page is not NotImplemented:
                    (output_doc,) = unpack_event_page(output_event_page)
            elif name == "datum":
                datum_page = pack_datum_page(cast(Datum, doc))
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
                for event in unpack_event_page(cast(EventPage, doc)):
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
                for datum in unpack_datum_page(cast(DatumPage, doc)):
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

    def start(self, doc: RunStart) -> Optional[RunStart]:
        return NotImplemented

    def stop(self, doc: RunStop) -> Optional[RunStop]:
        return NotImplemented

    def descriptor(self, doc: EventDescriptor) -> Optional[EventDescriptor]:
        return NotImplemented

    def resource(self, doc: Resource) -> Optional[Resource]:
        return NotImplemented

    def event(self, doc: Event) -> Event:
        return NotImplemented

    def datum(self, doc: Datum) -> Datum:
        return NotImplemented

    def event_page(self, doc: EventPage) -> EventPage:
        return NotImplemented

    def datum_page(self, doc: DatumPage) -> Optional[DatumPage]:
        return NotImplemented

    def stream_datum(self, doc: StreamDatum) -> Optional[StreamDatum]:
        return NotImplemented

    def stream_resource(self, doc: StreamResource) -> Optional[StreamResource]:
        return NotImplemented

    def bulk_events(self, doc: dict) -> None:
        # Do not modify this in a subclass. Use event_page.
        warnings.warn(
            "The document type 'bulk_events' has been deprecated in favor of "
            "'event_page', whose structure is a transpose of 'bulk_events'."
        )
        for page in bulk_events_to_event_pages(doc):
            self.event_page(page)

    def bulk_datum(self, doc: dict) -> None:
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

    def __init__(self) -> None:
        super().__init__()
        self._start_doc: Optional[dict] = None
        self._descriptors: dict = dict()

    def __call__(
        self, name: str, doc: dict, validate: bool = False
    ) -> Tuple[str, dict]:
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
                    "SingleRunDocumentRouter associated with start document "
                    f'{self._start_doc["uid"]} '
                    f'received a second start document with uid {doc["uid"]}'
                )
        elif name == "descriptor":
            assert isinstance(self._start_doc, dict)
            if doc["run_start"] == self._start_doc["uid"]:
                self._descriptors[doc["uid"]] = doc
            else:
                raise EventModelValueError(
                    "SingleRunDocumentRouter associated with start document "
                    f'{self._start_doc["uid"]} '
                    f'received a descriptor {doc["uid"]} associated with '
                    f'start document {doc["run_start"]}'
                )
        # Defer to superclass for dispatch/processing.
        return super().__call__(name, doc, validate=validate)

    def get_start(self) -> dict:
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

    def get_descriptor(self, doc: dict) -> EventDescriptor:
        """Convenience method returning the descriptor associated with the
        specified document.

        Parameters
        ----------
        doc : dict
            event-model document

        Returns
        -------
        descriptor document : EventDescriptor
        """
        if "descriptor" not in doc:
            raise EventModelValueError(
                f"document is not associated with a descriptor:\n{doc}"
            )
        elif doc["descriptor"] not in self._descriptors:
            raise EventModelValueError(
                "SingleRunDocumentRouter has not processed a descriptor with "
                f'uid {doc["descriptor"]}'
            )

        return self._descriptors[doc["descriptor"]]

    def get_stream_name(self, doc: dict) -> str:
        """Convenience method returning the name of the stream for the
        specified document.

        Parameters
        ----------
        doc : dict
            event-model document

        Returns
        -------
        stream name : str
        """
        return str(self.get_descriptor(doc).get("name"))


class HandlerRegistryView(collections.abc.Mapping):
    def __init__(self, handler_registry: dict) -> None:
        self._handler_registry = handler_registry

    def __repr__(self) -> str:
        return f"HandlerRegistryView({self._handler_registry!r})"

    def __getitem__(self, key: str) -> str:
        return self._handler_registry[key]

    def __iter__(self) -> Generator:
        yield from self._handler_registry

    def __len__(self) -> int:
        return len(self._handler_registry)

    def __setitem__(self, key: str, val: Any) -> None:
        raise EventModelTypeError(
            "The handler registry cannot be edited directly. "
            "Instead, use the method Filler.register_handler."
        )

    def __delitem__(self, key: str) -> None:
        raise EventModelTypeError(
            "The handler registry cannot be edited directly. "
            "Instead, use the method Filler.deregister_handler."
        )


# A "coercion funcion" is a hook that Filler can use to, for example, ensure
# all the external data read in my handlers is an *actual* numpy array as
# opposed to some other array-like such as h5py.Dataset or dask.array.Array,
# or wrap every result is dask.array.from_array(...).
#
# It has access to the handler_class as it is registered and to some state
# provided by the Filler (more on that below). It is expected to return
# something that is API-compatible with handler_class.  That might be
# handler_class itself (a no-op), a subclass, or an altogether different class
# with the same API. See example below.
#
# The "state provided by the Filler", mentioned above is passed into the
# coercion functions below as ``filler_state``. It is a namespace containing
# information that may be useful for the coercion functions.  Currently, it has
# ``filler_state.descriptor`` and ``filler_state.key``. More may be added in
# the future if the need arises. Ultimately, this is necessary because Resource
# documents don't know the shape and dtype of the data that they reference.
# That situation could be improved in the future; to some degree this is a
# work-around.
#
# As an implementation detail, the ``filler_state`` is a ``threading.local``
# object to ensure that filling is thread-safe.
#
# Third-party libraries can register custom coercion options via the
# register_coercion function below. For example, databroker uses this to
# register a 'delayed' option. This avoids introducing dependency on a specific
# delayed-computation framework (e.g. dask) in event-model itself.


def as_is(handler_class, filler_state) -> Type:
    "A no-op coercion function that returns handler_class unchanged."
    return handler_class


@no_type_check
def force_numpy(handler_class: Type, filler_state) -> Any:
    "A coercion that makes handler_class.__call__ return actual numpy.ndarray."

    class Subclass(handler_class):
        def __call__(self, *args, **kwargs):
            raw_result = super().__call__(*args, **kwargs)
            result_as_array = numpy.asarray(raw_result)
            return result_as_array

    Subclass.__name__ = f"Subclassed{handler_class.__name__}"
    Subclass.__qualname__ = f"Subclassed{handler_class.__qualname__}"
    return Subclass


# maps coerce option to corresponding coercion function
_coercion_registry = {"as_is": as_is, "force_numpy": force_numpy}


def register_coercion(name: str, func: Callable, overwrite: bool = False) -> None:
    """
    Register a new option for :class:`Filler`'s ``coerce`` argument.

    This is an advanced feature. See source code for comments and examples.

    Parameters
    ----------
    name : string
        The new value for ``coerce`` that will invoke this function.
    func : callable
        Expected signature::

            func(filler, handler_class) -> handler_class
    overwrite : boolean, optional
        False by default. Name collissions will raise ``EventModelValueError``
        unless this is set to ``True``.
    """

    if name in _coercion_registry and not overwrite:
        # If we are re-registering the same object, there is no problem.
        original = _coercion_registry[name]
        if original is func:
            return
        raise EventModelValueError(
            f"The coercion function {func} could not be registered for the "
            f"name {name} because {_coercion_registry[name]} is already "
            f"registered. Use overwrite=True to force it."
        )
    _coercion_registry[name] = func


register_coersion = register_coercion  # back-compat for a spelling mistake


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
    stream_resource_cache : dict, optional
        A cache of StreamResource documents. If None, a dict is used.
    stream_datum_cache : dict, optional
        A cache of StreamDatum documents. If None, a dict is used.
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
        handler_registry: dict,
        *,
        include: Optional[Iterable] = None,
        exclude: Optional[Iterable] = None,
        root_map: Optional[dict] = None,
        coerce: str = "as_is",
        handler_cache: Optional[dict] = None,
        resource_cache: Optional[dict] = None,
        datum_cache: Optional[dict] = None,
        descriptor_cache: Optional[dict] = None,
        stream_resource_cache: Optional[dict] = None,
        stream_datum_cache: Optional[dict] = None,
        inplace: Optional[bool] = None,
        retry_intervals: List = [
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
        ],
    ) -> None:
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
        self._unpatched_handler_registry: dict = {}
        self._handler_registry: dict = {}
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
        if stream_resource_cache is None:
            stream_resource_cache = self.get_default_stream_resource_cache()
        if stream_datum_cache is None:
            stream_datum_cache = self.get_default_stream_datum_cache()
        self._handler_cache = handler_cache
        self._resource_cache = resource_cache
        self._datum_cache = datum_cache
        self._descriptor_cache = descriptor_cache
        self._stream_resource_cache = stream_resource_cache
        self._stream_datum_cache = stream_datum_cache
        if retry_intervals is None:
            retry_intervals = []
        self.retry_intervals = retry_intervals
        self._closed = False

    def __eq__(self, other: Any) -> bool:
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
            and type(self._stream_resource_cache) is type(other._stream_resource_cache)
            and type(self._stream_datum_cache) is type(other._stream_datum_cache)
            and self.retry_intervals == other.retry_intervals
        )

    def __getstate__(self) -> dict:
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
            stream_resource_cache=self._stream_resource_cache,
            stream_datum_cache=self._stream_datum_cache,
            retry_intervals=self.retry_intervals,
        )

    def __setstate__(self, d: dict) -> None:
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
        self._stream_resource_cache = d["stream_resource_cache"]
        self._stream_datum_cache = d["stream_datum_cache"]
        retry_intervals = d["retry_intervals"]
        if retry_intervals is None:
            retry_intervals = []
        self._retry_intervals = retry_intervals
        self._closed = False

    @property
    def retry_intervals(self) -> List:
        return self._retry_intervals

    @retry_intervals.setter
    def retry_intervals(self, value: Any) -> None:
        self._retry_intervals = list(value)

    def __repr__(self) -> str:
        return "<Filler>" if not self._closed else "<Closed Filler>"

    @staticmethod
    def get_default_resource_cache() -> dict:
        return {}

    @staticmethod
    def get_default_descriptor_cache() -> dict:
        return {}

    @staticmethod
    def get_default_datum_cache() -> dict:
        return {}

    @staticmethod
    def get_default_handler_cache() -> dict:
        return {}

    @staticmethod
    def get_default_stream_datum_cache() -> dict:
        return {}

    @staticmethod
    def get_default_stream_resource_cache() -> dict:
        return {}

    @property
    def inplace(self) -> bool:
        return self._inplace

    def clone(
        self,
        handler_registry: Optional[dict] = None,
        *,
        root_map: Optional[dict] = None,
        coerce: Optional[str] = None,
        handler_cache: Optional[dict] = None,
        resource_cache: Optional[dict] = None,
        datum_cache: Optional[dict] = None,
        descriptor_cache: Optional[dict] = None,
        stream_resource_cache: Optional[dict] = None,
        stream_datum_cache: Optional[dict] = None,
        inplace: Optional[bool] = None,
        retry_intervals: Optional[List] = None,
    ) -> "Filler":
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
            stream_resource_cache=stream_resource_cache,
            stream_datum_cache=stream_datum_cache,
            inplace=inplace,
            retry_intervals=retry_intervals,
        )

    def register_handler(
        self, spec: str, handler: Any, overwrite: bool = False
    ) -> None:
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

    def deregister_handler(self, spec: str) -> Any:
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

    def resource(self, doc: Resource) -> Resource:
        # Defer creating the handler instance until we actually need it, when
        # we fill the first Event field that requires this Resource.
        self._resource_cache[doc["uid"]] = doc
        return doc

    # Handlers operate document-wise, so we'll explode pages into individual
    # documents.

    def datum_page(self, doc: DatumPage) -> DatumPage:
        datum = self.datum  # Avoid attribute lookup in hot loop.
        for datum_doc in unpack_datum_page(doc):
            datum(datum_doc)
        return doc

    def datum(self, doc: Datum) -> Datum:
        self._datum_cache[doc["datum_id"]] = doc
        return doc

    def stream_resource(self, doc: StreamResource) -> StreamResource:
        self._stream_resource_cache[doc["uid"]] = doc
        return doc

    def stream_datum(self, doc: StreamDatum) -> StreamDatum:
        self._stream_datum_cache[doc["uid"]] = doc
        return doc

    def event_page(self, doc: EventPage) -> EventPage:
        # TODO We may be able to fill a page in place, and that may be more
        # efficient than unpacking the page in to Events, filling them, and the
        # re-packing a new page. But that seems tricky in general since the
        # page may be implemented as a DataFrame or dict, etc.
        filled_doc = self.fill_event_page(
            doc, include=self.include, exclude=self.exclude
        )
        return filled_doc

    def event(self, doc: Event) -> Event:
        filled_doc = self.fill_event(doc, include=self.include, exclude=self.exclude)
        return filled_doc

    def fill_event_page(
        self,
        doc: EventPage,
        include: Optional[Iterable] = None,
        exclude: Optional[Iterable] = None,
        inplace: Optional[bool] = None,
    ) -> EventPage:
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

    def get_handler(self, resource: Resource) -> Any:
        """
        Return a new Handler instance for this Resource.

        Parameters
        ----------
        resource: Resource

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

    def _get_handler_maybe_cached(self, resource: Resource) -> Any:
        "Get a cached handler for this resource or make one and cache it."
        key = (resource["uid"], resource["spec"])
        try:
            handler = self._handler_cache[key]
        except KeyError:
            handler = self.get_handler(resource)
            self._handler_cache[key] = handler
        return handler

    def fill_event(
        self,
        doc,
        include: Optional[Iterable] = None,
        exclude: Optional[Iterable] = None,
        inplace: Optional[bool] = None,
    ) -> Any:
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

    def descriptor(self, doc: EventDescriptor) -> EventDescriptor:
        self._descriptor_cache[doc["uid"]] = doc
        return doc

    def __enter__(self):
        return self

    @no_type_check
    def close(self) -> None:
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
    def closed(self) -> bool:
        return self._closed

    def clear_handler_cache(self) -> None:
        """
        Clear any cached handler instances.

        This operation may free significant memory, depending on the
        implementation of the handlers.
        """
        self._handler_cache.clear()

    def clear_document_caches(self) -> None:
        """
        Clear any cached documents.
        """
        self._resource_cache.clear()
        self._descriptor_cache.clear()
        self._datum_cache.clear()

    def __exit__(self, *exc_details) -> None:
        self.close()

    def __call__(
        self, name: str, doc: dict, validate: bool = False
    ) -> Tuple[str, dict]:
        if self._closed:
            raise EventModelRuntimeError(
                "This Filler has been closed and is no longer usable."
            )
        return super().__call__(name, doc, validate)


class EventModelError(Exception): ...


def _attempt_with_retries(
    func,
    args,
    kwargs,
    intervals: Iterable,
    error_to_catch: Type[OSError],
    error_to_raise: EventModelError,
) -> Any:
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


class NoFiller(Filler):
    """
    This does not fill the documents; it merely validates them.

    It checks that all the references between the documents are resolvable and
    *could* be filled. This is useful when the filling will be done later, as
    a delayed computation, but we want to make sure in advance that we have all
    the information that we will need when that computation occurs.
    """

    def __init__(self, *args, **kwargs) -> None:
        # Do not make Filler make copies because we are not going to alter the
        # documents anyway.
        kwargs.setdefault("inplace", True)
        super().__init__(*args, **kwargs)

    def fill_event_page(
        self,
        doc: EventPage,
        include: Optional[Iterable] = None,
        exclude: Optional[Iterable] = None,
        *kwargs,
    ) -> EventPage:
        filled_events = []
        for event_doc in unpack_event_page(doc):
            filled_events.append(
                self.fill_event(
                    event_doc, include=include, exclude=exclude, inplace=True
                )
            )
        filled_doc = pack_event_page(*filled_events)
        return filled_doc

    def fill_event(
        self,
        doc: Event,
        include: Optional[Iterable] = None,
        exclude: Optional[Iterable] = None,
        inplace: Optional[bool] = None,
    ) -> Event:
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
        if set to True, fill everything and raise
        ``UndefinedAssetSpecification`` if some unknown spec is encountered.
    """

    def __init__(
        self,
        factories,
        handler_registry: Optional[dict] = None,
        *,
        root_map: Optional[dict] = None,
        filler_class: Type[Filler] = Filler,
        fill_or_fail: bool = False,
    ) -> None:
        self.factories = factories
        self.handler_registry = handler_registry or {}
        self.filler_class = filler_class
        self.fill_or_fail = fill_or_fail
        self.root_map = root_map

        # Map RunStart UID to "subfactory" functions that want all
        # EventDescriptors from that run.
        self._subfactories: defaultdict = defaultdict(list)

        # Callbacks that want all the documents from a given run, keyed on
        # RunStart UID.
        self._factory_cbs_by_start: defaultdict = defaultdict(list)

        # Callbacks that want all the documents from a given run, keyed on
        # each EventDescriptor UID in the run.
        self._factory_cbs_by_descriptor: defaultdict = defaultdict(list)

        # Callbacks that want documents related to a given EventDescriptor,
        # keyed on EventDescriptor UID.
        self._subfactory_cbs_by_descriptor: defaultdict = defaultdict(list)

        # Callbacks that want documents related to a given EventDescriptor,
        # keyed on the RunStart UID referenced by that EventDescriptor.
        self._subfactory_cbs_by_start: defaultdict = defaultdict(list)

        # Map RunStart UID to RunStart document. This is used to send
        # RunStart documents to subfactory callbacks.
        self._start_to_start_doc: dict = dict()

        # Map RunStart UID to the list EventDescriptor. This is used to
        # facilitate efficient cleanup of the caches above.
        self._start_to_descriptors: defaultdict = defaultdict(list)

        # Map EventDescriptor UID to RunStart UID. This is used for looking up
        # Fillers.
        self._descriptor_to_start: dict = {}

        # Map Resource UID to RunStart UID.
        self._resources: dict = {}
        self._stream_resources: dict = {}

        # Old-style Resources that do not have a RunStart UID
        self._unlabeled_resources: deque = deque(maxlen=10000)

        # Map Runstart UID to instances of self.filler_class.
        self._fillers: dict = {}

    def __repr__(self):
        return (
            "RunRouter([\n"
            + "\n".join(f"    {factory}" for factory in self.factories)
            + "])"
        )

    def start(self, start_doc: RunStart) -> None:
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

    def descriptor(self, descriptor_doc: EventDescriptor) -> None:
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

    def event_page(self, doc: EventPage):
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

    def datum_page(self, doc: DatumPage) -> None:
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

    def stream_datum(self, doc: StreamDatum) -> None:
        resource_uid = doc["stream_resource"]
        start_uid = self._stream_resources[resource_uid]
        self._fillers[start_uid].stream_datum(doc)
        for callback in self._factory_cbs_by_start[start_uid]:
            callback("stream_datum", doc)
        for callback in self._subfactory_cbs_by_start[start_uid]:
            callback("stream_datum", doc)

    def resource(self, doc: Resource) -> None:
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

    def stream_resource(self, doc: StreamResource) -> None:
        start_uid = doc["run_start"]  # No need for Try
        self._fillers[start_uid].stream_resource(doc)
        self._stream_resources[doc["uid"]] = doc["run_start"]
        for callback in self._factory_cbs_by_start[start_uid]:
            callback("stream_resource", doc)
        for callback in self._subfactory_cbs_by_start[start_uid]:
            callback("stream_resource", doc)

    def stop(self, doc: RunStop) -> None:
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


# Here we define subclasses of all of the built-in Python exception types (as
# needed, not a comprehensive list) so that all errors raised *directly* by
# event_model also inhereit from EventModelError as well as the appropriate
# built-in type. This means, for example, that `EventModelValueError` can be
# caught by `except ValueError:` or by `except EventModelError:`. This can be
# useful for higher-level libraries and for debugging.


class EventModelKeyError(EventModelError, KeyError): ...


class EventModelValueError(EventModelError, ValueError): ...


class EventModelRuntimeError(EventModelError, RuntimeError): ...


class EventModelTypeError(EventModelError, TypeError): ...


class EventModelValidationError(EventModelError): ...


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

    def __init__(self, key: Any, message: str) -> None:
        self.key = key
        self.message = message


class DuplicateHandler(EventModelRuntimeError):
    """raised when a handler is already registered for a given spec"""

    ...


class InvalidData(EventModelError):
    """raised when the data is invalid"""

    ...


class MismatchedDataKeys(InvalidData):
    """
    Raised when any data keys structures are out of sync. This includes,
    event['data'].keys(), descriptor['data_keys'].keys(),
    event['timestamp'].keys(), event['filled'].keys()
    """

    ...


SCHEMA_PATH = "schemas"
SCHEMA_NAMES = {
    DocumentNames.start: "schemas/run_start.json",
    DocumentNames.stop: "schemas/run_stop.json",
    DocumentNames.event: "schemas/event.json",
    DocumentNames.event_page: "schemas/event_page.json",
    DocumentNames.descriptor: "schemas/event_descriptor.json",
    DocumentNames.datum: "schemas/datum.json",
    DocumentNames.datum_page: "schemas/datum_page.json",
    DocumentNames.resource: "schemas/resource.json",
    DocumentNames.stream_datum: "schemas/stream_datum.json",
    DocumentNames.stream_resource: "schemas/stream_resource.json",
    # DEPRECATED:
    DocumentNames.bulk_events: "schemas/bulk_events.json",
    DocumentNames.bulk_datum: "schemas/bulk_datum.json",
}
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    ref = importlib_resources.files("event_model") / filename
    with ref.open() as f:
        schemas[name] = json.load(f)


def _is_array(checker, instance):
    return (
        jsonschema.validators.Draft7Validator.TYPE_CHECKER.is_type(instance, "array")
        or isinstance(instance, tuple)
        or hasattr(instance, "__array__")
    )


_array_type_checker = jsonschema.validators.Draft7Validator.TYPE_CHECKER.redefine(
    "array", _is_array
)

_Validator = jsonschema.validators.extend(
    jsonschema.validators.Draft7Validator, type_checker=_array_type_checker
)

schema_validators = {
    name: _Validator(schema=schema) for name, schema in schemas.items()
}


@dataclass
class ComposeDatum:
    resource: Resource
    counter: Iterator

    def __call__(self, datum_kwargs: Dict[str, Any], validate: bool = True) -> Datum:
        resource_uid = self.resource["uid"]
        doc = Datum(
            resource=resource_uid,
            datum_kwargs=datum_kwargs,
            datum_id="{}/{}".format(resource_uid, next(self.counter)),
        )
        if validate:
            schema_validators[DocumentNames.datum].validate(doc)
        return doc


def compose_datum(
    *,
    resource: Resource,
    counter: Iterator,
    datum_kwargs: Dict[str, Any],
    validate: bool = True,
) -> Datum:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    return ComposeDatum(resource, counter)(datum_kwargs, validate=validate)


@dataclass
class ComposeDatumPage:
    resource: Resource
    counter: Iterator

    def __call__(self, datum_kwargs: dict, validate: bool = True) -> DatumPage:
        resource_uid = self.resource["uid"]
        any_column, *_ = datum_kwargs.values()
        N = len(any_column)
        doc = DatumPage(
            resource=resource_uid,
            datum_kwargs=datum_kwargs,
            datum_id=[
                "{}/{}".format(resource_uid, next(self.counter)) for _ in range(N)
            ],
        )
        if validate:
            schema_validators[DocumentNames.datum_page].validate(doc)
        return doc


def compose_datum_page(
    *,
    resource: Resource,
    counter: Iterator,
    datum_kwargs: Dict[str, List[Any]],
    validate: bool = True,
) -> DatumPage:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    return ComposeDatumPage(resource, counter)(datum_kwargs, validate=validate)


@dataclass
class ComposeResourceBundle:
    resource_doc: Resource
    compose_datum: ComposeDatum
    compose_datum_page: ComposeDatumPage

    # iter for backwards compatibility
    def __iter__(self) -> Iterator:
        return iter(
            (
                self.resource_doc,
                self.compose_datum,
                self.compose_datum_page,
            )
        )


PATH_SEMANTICS: Dict[str, Literal["posix", "windows"]] = {
    "posix": "posix",
    "nt": "windows",
}
default_path_semantics: Literal["posix", "windows"] = PATH_SEMANTICS[os.name]


@dataclass
class ComposeResource:
    start: Optional[RunStart]

    def __call__(
        self,
        spec: str,
        root: str,
        resource_path: str,
        resource_kwargs: Dict[str, Any],
        path_semantics: Literal["posix", "windows"] = default_path_semantics,
        uid: Optional[str] = None,
        validate: bool = True,
    ) -> ComposeResourceBundle:
        if uid is None:
            uid = str(uuid.uuid4())

        doc = Resource(
            path_semantics=path_semantics,
            uid=uid,
            spec=spec,
            root=root,
            resource_kwargs=resource_kwargs,
            resource_path=resource_path,
        )

        if self.start:
            doc["run_start"] = self.start["uid"]

        if validate:
            schema_validators[DocumentNames.resource].validate(doc)

        counter = itertools.count()
        return ComposeResourceBundle(
            doc,
            ComposeDatum(resource=doc, counter=counter),
            ComposeDatumPage(resource=doc, counter=counter),
        )


def compose_resource(
    *,
    spec: str,
    root: str,
    resource_path: str,
    resource_kwargs: Dict[str, Any],
    path_semantics: Literal["posix", "windows"] = default_path_semantics,
    start: Optional[RunStart] = None,
    uid: Optional[str] = None,
    validate: bool = True,
) -> ComposeResourceBundle:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    return ComposeResource(start)(
        spec,
        root,
        resource_path,
        resource_kwargs,
        path_semantics=path_semantics,
        uid=uid,
        validate=validate,
    )


@dataclass
class ComposeStreamDatum:
    stream_resource: StreamResource
    counter: Iterator

    def __call__(
        self,
        indices: StreamRange,
        seq_nums: Optional[StreamRange] = None,
        descriptor: Optional[EventDescriptor] = None,
        validate: bool = True,
    ) -> StreamDatum:
        resource_uid = self.stream_resource["uid"]

        # If the seq_nums aren't passed in then the bluesky
        # bundler will keep track of them
        if not seq_nums:
            seq_nums = StreamRange(start=0, stop=0)

        doc = StreamDatum(
            stream_resource=resource_uid,
            uid=f"{resource_uid}/{next(self.counter)}",
            seq_nums=seq_nums,
            indices=indices,
            descriptor=descriptor["uid"] if descriptor else "",
        )

        if validate:
            schema_validators[DocumentNames.stream_datum].validate(doc)

        return doc


def compose_stream_datum(
    *,
    stream_resource: StreamResource,
    counter: Iterator,
    seq_nums: StreamRange,
    indices: StreamRange,
    validate: bool = True,
) -> StreamDatum:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    warnings.warn(
        "compose_stream_datum() will be removed in the minor version.",
        DeprecationWarning,
    )
    return ComposeStreamDatum(stream_resource, counter)(
        seq_nums,
        indices,
        validate=validate,
    )


@dataclass
class ComposeStreamResourceBundle:
    stream_resource_doc: StreamResource
    compose_stream_datum: ComposeStreamDatum

    # iter for backwards compatibility
    def __iter__(self) -> Iterator:
        return iter(
            (
                self.stream_resource_doc,
                self.compose_stream_datum,
            )
        )


@dataclass
class ComposeStreamResource:
    start: Optional[RunStart] = None

    def __call__(
        self,
        mimetype: str,
        uri: str,
        data_key: str,
        parameters: Dict[str, Any],
        uid: Optional[str] = None,
        validate: bool = True,
    ) -> ComposeStreamResourceBundle:
        if uid is None:
            uid = str(uuid.uuid4())

        doc = StreamResource(
            uid=uid,
            data_key=data_key,
            mimetype=mimetype,
            uri=uri,
            parameters=parameters,
        )

        if self.start:
            doc["run_start"] = self.start["uid"]

        if validate:
            schema_validators[DocumentNames.stream_resource].validate(doc)

        return ComposeStreamResourceBundle(
            doc,
            ComposeStreamDatum(
                stream_resource=doc,
                counter=itertools.count(),
            ),
        )


def compose_stream_resource(
    *,
    mimetype: str,
    uri: str,
    data_key: str,
    parameters: Dict[str, Any],
    start: Optional[RunStart] = None,
    uid: Optional[str] = None,
    validate: bool = True,
) -> ComposeStreamResourceBundle:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    return ComposeStreamResource(start=start)(
        mimetype,
        uri,
        data_key,
        parameters,
        uid=uid,
        validate=validate,
    )


@dataclass
class ComposeStop:
    start: RunStart
    event_counters: Dict[str, int]
    poison_pill: List

    def __call__(
        self,
        exit_status: Literal["success", "abort", "fail"] = "success",
        reason: str = "",
        uid: Optional[str] = None,
        time: Optional[float] = None,
        validate: bool = True,
    ) -> RunStop:
        if self.poison_pill:
            raise EventModelError(
                "Already composed a RunStop document for run "
                "{!r}.".format(self.start["uid"])
            )
        self.poison_pill.append(object())
        if uid is None:
            uid = str(uuid.uuid4())
        if time is None:
            time = ttime.time()
        doc = RunStop(
            uid=uid,
            time=time,
            run_start=self.start["uid"],
            exit_status=exit_status,
            reason=reason,
            num_events={k: v - 1 for k, v in self.event_counters.items()},
        )
        if validate:
            schema_validators[DocumentNames.stop].validate(doc)
        return doc


def compose_stop(
    *,
    start: RunStart,
    event_counters: Dict[str, int],
    poison_pill: List,
    exit_status: Literal["success", "abort", "fail"] = "success",
    reason: str = "",
    uid: Optional[str] = None,
    time: Optional[float] = None,
    validate: bool = True,
) -> RunStop:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    return ComposeStop(
        start=start,
        event_counters=event_counters,
        poison_pill=poison_pill,
    )(exit_status=exit_status, reason=reason, uid=uid, time=time, validate=validate)


def length_of_value(dictionary: Dict[str, List], error_msg: str) -> Optional[int]:
    length = None
    for k, v in dictionary.items():
        v_len = len(v)
        if length is not None:
            if v_len != length:
                raise EventModelError(error_msg)
        length = v_len
    return length


@dataclass
class ComposeEventPage:
    descriptor: EventDescriptor
    event_counters: Dict[str, int]

    def __call__(
        self,
        data: Dict[str, List],
        timestamps: Dict[str, Any],
        seq_num: Optional[List[int]] = None,
        filled: Optional[Dict[str, List[Union[bool, str]]]] = None,
        uid: Optional[List] = None,
        time: Optional[List] = None,
        validate: bool = True,
    ) -> EventPage:
        timestamps_length = length_of_value(
            timestamps,
            "Cannot compose event_page: event_page contains `timestamps` "
            "list values of different lengths",
        )
        data_length = length_of_value(
            data,
            "Cannot compose event_page: event_page contains `data` "
            "lists of different lengths",
        )
        assert timestamps_length == data_length, (
            "Cannot compose event_page: the lists in `timestamps` are of a different "
            "length to those in `data`"
        )

        if seq_num is None:
            last_seq_num = self.event_counters[self.descriptor["name"]]
            seq_num = list(
                range(last_seq_num, len(next(iter(data.values()))) + last_seq_num)
            )
        N = len(seq_num)
        if uid is None:
            uid = [str(uuid.uuid4()) for _ in range(N)]
        if time is None:
            time = [ttime.time()] * N
        if filled is None:
            filled = {}
        doc = EventPage(
            uid=uid,
            time=time,
            data=data,
            timestamps=timestamps,
            seq_num=seq_num,
            filled=filled,
            descriptor=self.descriptor["uid"],
        )
        if validate:
            schema_validators[DocumentNames.event_page].validate(doc)

            if not (
                set(
                    keys_without_stream_keys(
                        self.descriptor["data_keys"], self.descriptor["data_keys"]
                    )
                )
                == set(keys_without_stream_keys(data, self.descriptor["data_keys"]))
                == set(
                    keys_without_stream_keys(timestamps, self.descriptor["data_keys"])
                )
            ):
                raise EventModelValidationError(
                    'These sets of keys must match (other than "STREAM:" keys):\n'
                    "event['data'].keys(): {}\n"
                    "event['timestamps'].keys(): {}\n"
                    "descriptor['data_keys'].keys(): {}\n".format(
                        data.keys(),
                        timestamps.keys(),
                        self.descriptor["data_keys"].keys(),
                    )
                )
            if set(filled) - set(data):
                raise EventModelValidationError(
                    "Keys in event['filled'] {} must be a subset of those in "
                    "event['data'] {}".format(filled.keys(), data.keys())
                )
        self.event_counters[self.descriptor["name"]] += len(seq_num)
        return doc


def compose_event_page(
    *,
    descriptor: EventDescriptor,
    event_counters: Dict[str, int],
    data: Dict[str, List],
    timestamps: Dict[str, Any],
    seq_num: List[int],
    filled: Optional[Dict[str, List[Union[bool, str]]]] = None,
    uid: Optional[List] = None,
    time: Optional[List] = None,
    validate: bool = True,
) -> EventPage:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    return ComposeEventPage(descriptor, event_counters)(
        data,
        timestamps,
        seq_num=seq_num,
        filled=filled,
        uid=uid,
        time=time,
        validate=validate,
    )


def keys_without_stream_keys(dictionary, descriptor_data_keys):
    return [
        key
        for key in dictionary.keys()
        if (
            "external" not in descriptor_data_keys[key]
            or descriptor_data_keys[key]["external"] != "STREAM:"
        )
    ]


@dataclass
class ComposeEvent:
    descriptor: EventDescriptor
    event_counters: Dict[str, int]

    def __call__(
        self,
        data: dict,
        timestamps: dict,
        seq_num: Optional[int] = None,
        filled: Optional[Dict[str, Union[bool, str]]] = None,
        uid: Optional[str] = None,
        time: Optional[float] = None,
        validate: bool = True,
    ) -> Event:
        if seq_num is None:
            seq_num = self.event_counters[self.descriptor["name"]]
        if uid is None:
            uid = str(uuid.uuid4())
        if time is None:
            time = ttime.time()
        if filled is None:
            filled = {}
        doc = Event(
            uid=uid,
            time=time,
            data=data,
            timestamps=timestamps,
            seq_num=seq_num,
            filled=filled,
            descriptor=self.descriptor["uid"],
        )
        if validate:
            schema_validators[DocumentNames.event].validate(doc)

            if not (
                set(
                    keys_without_stream_keys(
                        self.descriptor["data_keys"], self.descriptor["data_keys"]
                    )
                )
                == set(keys_without_stream_keys(data, self.descriptor["data_keys"]))
                == set(
                    keys_without_stream_keys(timestamps, self.descriptor["data_keys"])
                )
            ):
                raise EventModelValidationError(
                    'These sets of keys must match (other than "STREAM:" keys):\n'
                    "event['data'].keys(): {}\n"
                    "event['timestamps'].keys(): {}\n"
                    "descriptor['data_keys'].keys(): {}\n".format(
                        data.keys(),
                        timestamps.keys(),
                        self.descriptor["data_keys"].keys(),
                    )
                )
            if set(filled) - set(data):
                raise EventModelValidationError(
                    "Keys in event['filled'] {} must be a subset of those in "
                    "event['data'] {}".format(filled.keys(), data.keys())
                )
        self.event_counters[self.descriptor["name"]] = seq_num + 1
        return doc


def compose_event(
    *,
    descriptor: EventDescriptor,
    event_counters: Dict[str, int],
    data: Dict[str, Any],
    timestamps: Dict[str, Any],
    seq_num: int,
    filled: Optional[Dict[str, Union[bool, str]]] = None,
    uid: Optional[str] = None,
    time: Optional[float] = None,
    validate: bool = True,
) -> Event:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    return ComposeEvent(descriptor, event_counters)(
        data,
        timestamps,
        seq_num=seq_num,
        filled=filled,
        uid=uid,
        time=time,
        validate=validate,
    )


@dataclass
class ComposeDescriptorBundle:
    descriptor_doc: EventDescriptor
    compose_event: ComposeEvent
    compose_event_page: ComposeEventPage

    def __iter__(self) -> Iterator:
        return iter(
            (
                self.descriptor_doc,
                self.compose_event,
                self.compose_event_page,
            )
        )


@dataclass
class ComposeDescriptor:
    start: RunStart
    streams: dict
    event_counters: Dict[str, int]

    def __call__(
        self,
        name,
        data_keys,
        hints=None,
        configuration=None,
        object_keys=None,
        time=None,
        uid=None,
        validate=True,
    ) -> ComposeDescriptorBundle:
        if time is None:
            time = ttime.time()
        if uid is None:
            uid = str(uuid.uuid4())
        if hints is None:
            hints = {}
        if configuration is None:
            configuration = {}
        if object_keys is None:
            object_keys = {}

        doc = EventDescriptor(
            configuration=configuration,
            data_keys=data_keys,
            name=name,
            object_keys=object_keys,
            run_start=self.start["uid"],
            time=time,
            uid=uid,
            hints=hints,
        )
        if validate:
            if name in self.streams and self.streams[name] != set(data_keys):
                raise EventModelValidationError(
                    "A descriptor with the name {} has already been composed with "
                    "data_keys {}. The requested data_keys were {}. All "
                    "descriptors in a given stream must have the same "
                    "data_keys.".format(name, self.streams[name], set(data_keys))
                )
            schema_validators[DocumentNames.descriptor].validate(doc)

        if name not in self.streams:
            self.streams[name] = set(data_keys)
            self.event_counters[name] = 1

        return ComposeDescriptorBundle(
            descriptor_doc=doc,
            compose_event=ComposeEvent(
                descriptor=doc, event_counters=self.event_counters
            ),
            compose_event_page=ComposeEventPage(
                descriptor=doc, event_counters=self.event_counters
            ),
        )


def compose_descriptor(
    *,
    start: RunStart,
    streams: Dict[str, Iterable],
    event_counters: Dict[str, int],
    name: str,
    data_keys: Dict[str, DataKey],
    uid: Optional[str] = None,
    time: Optional[float] = None,
    object_keys: Optional[Dict[str, Any]] = None,
    configuration: Optional[Dict[str, Configuration]] = None,
    hints: Optional[PerObjectHint] = None,
    validate: bool = True,
) -> ComposeDescriptorBundle:
    """
    Here for backwards compatibility, the Compose class is prefered.
    """
    return ComposeDescriptor(start, streams, event_counters)(
        name,
        data_keys,
        hints=hints,
        configuration=configuration,
        object_keys=object_keys,
        time=time,
        uid=uid,
        validate=validate,
    )


@dataclass
class ComposeRunBundle:
    """Extensible compose run bundle. This maintains backward compatibility
    by unpacking into a basic run bundle
    (start, compose_descriptor, compose_resource, stop).
    Further extensions are optional and require keyword referencing
    (i.e. compose_stream_resource).
    """

    start_doc: RunStart
    compose_descriptor: ComposeDescriptor
    compose_resource: ComposeResource
    compose_stop: ComposeStop
    compose_stream_resource: Optional[ComposeStreamResource] = None

    # iter for backwards compatibility
    def __iter__(self) -> Iterator:
        return iter(
            (
                self.start_doc,
                self.compose_descriptor,
                self.compose_resource,
                self.compose_stop,
            )
        )


def compose_run(
    *,
    uid: Optional[str] = None,
    time: Optional[float] = None,
    metadata: Optional[Dict] = None,
    validate: bool = True,
    event_counters: Optional[Dict[str, int]] = None,
) -> ComposeRunBundle:
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
    event_counters : dict, optional
        A dict for counting events, when an event is composed by any of the
        descriptors composed by this run, the element in this dict with the key of the
        descriptor name will be increased by 1.

    Returns
    -------
    ComposeRunBundle
    """
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    if metadata is None:
        metadata = {}

    # Define some mutable state to be shared internally by the closures composed
    # below.
    streams: Dict[str, Iterable] = {}
    if event_counters is None:
        event_counters = {}
    poison_pill: list = []

    doc = dict(uid=uid, time=time, **metadata)

    if validate:
        schema_validators[DocumentNames.start].validate(doc)

    return ComposeRunBundle(
        cast(RunStart, doc),
        ComposeDescriptor(
            start=cast(RunStart, doc), streams=streams, event_counters=event_counters
        ),
        ComposeResource(start=cast(RunStart, doc)),
        ComposeStop(
            start=cast(RunStart, doc),
            event_counters=event_counters,
            poison_pill=poison_pill,
        ),
        compose_stream_resource=ComposeStreamResource(start=cast(RunStart, doc)),
    )


def pack_event_page(*events: Event) -> EventPage:
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
    if not events:
        raise ValueError(
            "The pack_event_page() function was called with empty *args. "
            "Cannot create an EventPage from an empty collection of Events "
            "because the 'descriptor' field in an EventPage cannot be NULL."
        )
    time_list = []
    uid_list = []
    seq_num_list = []
    data_list = []
    filled_list = []
    timestamps_list = []
    for event in events:
        time_list.append(event["time"])
        uid_list.append(event["uid"])
        seq_num_list.append(event["seq_num"])
        filled_list.append(event.get("filled", {}))
        data_list.append(event["data"])
        timestamps_list.append(event["timestamps"])
    event_page = EventPage(
        time=time_list,
        uid=uid_list,
        seq_num=seq_num_list,
        descriptor=event["descriptor"],
        filled=_transpose_list_of_dicts(filled_list),
        data=_transpose_list_of_dicts(data_list),
        timestamps=_transpose_list_of_dicts(timestamps_list),
    )
    return event_page


def unpack_event_page(event_page: EventPage) -> Generator:
    """
    Transform an EventPage document into individual Event documents.

    Parameters
    ----------
    event_page : EventPage

    Yields
    ------
    event : Event
    """
    descriptor = event_page["descriptor"]
    data_list = _transpose_dict_of_lists(event_page["data"])
    timestamps_list = _transpose_dict_of_lists(event_page["timestamps"])
    filled_list = _transpose_dict_of_lists(event_page.get("filled", {}))
    for uid, time, seq_num, data, timestamps, filled in itertools.zip_longest(
        event_page["uid"],
        event_page["time"],
        event_page["seq_num"],
        data_list,
        timestamps_list,
        filled_list,
        fillvalue={},
    ):
        yield Event(
            descriptor=descriptor,
            uid=uid,
            time=time,
            seq_num=seq_num,
            data=data,
            timestamps=timestamps,
            filled=filled,
        )


def pack_datum_page(*datum: Datum) -> DatumPage:
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
    if not datum:
        raise ValueError(
            "The pack_datum_page() function was called with empty *args. "
            "Cannot create an DatumPage from an empty collection of Datum "
            "because the 'resource' field in a DatumPage cannot be NULL."
        )
    datum_id_list = []
    datum_kwarg_list = []
    for datum_ in datum:
        datum_id_list.append(datum_["datum_id"])
        datum_kwarg_list.append(datum_["datum_kwargs"])
    datum_page = DatumPage(
        resource=datum_["resource"],
        datum_id=datum_id_list,
        datum_kwargs=_transpose_list_of_dicts(datum_kwarg_list),
    )
    return datum_page


def unpack_datum_page(datum_page: DatumPage) -> Generator:
    """
    Transform a DatumPage document into individual Datum documents.

    Parameters
    ----------
    datum_page : DatumPage

    Yields
    ------
    datum : Datum
    """
    resource = datum_page["resource"]
    datum_kwarg_list = _transpose_dict_of_lists(datum_page["datum_kwargs"])
    datum_id: Any
    datum_kwargs: Any
    for datum_id, datum_kwargs in itertools.zip_longest(
        datum_page["datum_id"], datum_kwarg_list, fillvalue={}
    ):
        yield Datum(datum_id=datum_id, datum_kwargs=datum_kwargs, resource=resource)


def rechunk_event_pages(event_pages: Iterable, chunk_size: int) -> Generator:
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

    def page_chunks(page: dict, chunk_size: int, remainder: int) -> Generator:
        """
        Yields chunks of a event_page.
        The first chunk will be of size remainder, the following chunks will be
        of size chunk_size. The last chunk will be what ever is left over.
        """
        array_keys = ["seq_num", "time", "uid"]
        page_size = len(page["uid"])  # Number of events in the page.

        # Make a list of the chunk indexes.
        chunks = [(0, remainder)]
        chunks.extend(
            [(i, i + chunk_size) for i in range(remainder, page_size, chunk_size)]
        )

        for start, stop in chunks:
            yield {
                "descriptor": page["descriptor"],
                **{key: page[key][start:stop] for key in array_keys},
                "data": {
                    key: page["data"][key][start:stop] for key in page["data"].keys()
                },
                "timestamps": {
                    key: page["timestamps"][key][start:stop]
                    for key in page["timestamps"].keys()
                },
                "filled": {
                    key: page["filled"][key][start:stop]
                    for key in page["filled"].keys()
                },
            }

    for page in event_pages:
        new_chunks = page_chunks(page, chunk_size, remainder)
        for chunk in new_chunks:
            remainder -= len(chunk["uid"])  # Subtract the size of the chunk.
            chunk_list.append(chunk)
            if remainder == 0:
                yield merge_event_pages(chunk_list)
                remainder = chunk_size
                chunk_list = []
    if chunk_list:
        yield merge_event_pages(chunk_list)


def merge_event_pages(event_pages: Iterable[EventPage]) -> EventPage:
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

    doc = dict(
        descriptor=pages[0]["descriptor"],
        seq_num=list(
            itertools.chain.from_iterable([page["seq_num"] for page in pages])
        ),
        time=list(itertools.chain.from_iterable([page["time"] for page in pages])),
        uid=list(itertools.chain.from_iterable([page["uid"] for page in pages])),
        data={
            key: list(
                itertools.chain.from_iterable([page["data"][key] for page in pages])
            )
            for key in pages[0]["data"].keys()
        },
        timestamps={
            key: list(
                itertools.chain.from_iterable(
                    [page["timestamps"][key] for page in pages]
                )
            )
            for key in pages[0]["data"].keys()
        },
        filled={
            key: list(
                itertools.chain.from_iterable([page["filled"][key] for page in pages])
            )
            for key in pages[0]["filled"].keys()
        },
    )
    return cast(EventPage, doc)


def rechunk_datum_pages(datum_pages: Iterable, chunk_size: int) -> Generator:
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

    def page_chunks(page: dict, chunk_size: int, remainder: int) -> Generator:
        """
        Yields chunks of a datum_page.
        The first chunk will be of size remainder, the following chunks will be
        of size chunk_size. The last chunk will be what ever is left over.
        """

        array_keys = ["datum_id"]
        page_size = len(page["datum_id"])  # Number of datum in the page.

        # Make a list of the chunk indexes.
        chunks = [(0, remainder)]
        chunks.extend(
            [(i, i + chunk_size) for i in range(remainder, page_size, chunk_size)]
        )

        for start, stop in chunks:
            yield {
                "resource": page["resource"],
                **{key: page[key][start:stop] for key in array_keys},
                "datum_kwargs": {
                    key: page["datum_kwargs"][key][start:stop]
                    for key in page["datum_kwargs"].keys()
                },
            }

    for page in datum_pages:
        new_chunks = page_chunks(page, chunk_size, remainder)
        for chunk in new_chunks:
            remainder -= len(chunk["datum_id"])  # Subtract the size of the chunk.
            chunk_list.append(chunk)
            if remainder == 0:
                yield merge_datum_pages(chunk_list)
                remainder = chunk_size
                chunk_list = []
    if chunk_list:
        yield merge_datum_pages(chunk_list)


def merge_datum_pages(datum_pages: Iterable) -> DatumPage:
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

    array_keys = ["datum_id"]

    doc = dict(
        resource=pages[0]["resource"],
        **{
            key: list(itertools.chain.from_iterable([page[key] for page in pages]))
            for key in array_keys
        },
        datum_kwargs={
            key: list(
                itertools.chain.from_iterable(
                    [page["datum_kwargs"][key] for page in pages]
                )
            )
            for key in pages[0]["datum_kwargs"].keys()
        },
    )
    return cast(DatumPage, doc)


def bulk_events_to_event_pages(bulk_events: dict) -> list:
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
    event_pages: dict = {}  # descriptor uid mapped to page
    for events in bulk_events.values():
        for event in events:
            descriptor = event["descriptor"]
            try:
                page = event_pages[descriptor]
            except KeyError:
                page = {"time": [], "uid": [], "seq_num": [], "descriptor": descriptor}
                page["data"] = {k: [] for k in event["data"]}
                page["timestamps"] = {k: [] for k in event["timestamps"]}
                page["filled"] = {k: [] for k in event.get("filled", {})}
                event_pages[descriptor] = page
            page["uid"].append(event["uid"])
            page["time"].append(event["time"])
            page["seq_num"].append(event["seq_num"])
            page_data = page["data"]
            for k, v in event["data"].items():
                page_data[k].append(v)
            page_timestamps = page["timestamps"]
            for k, v in event["timestamps"].items():
                page_timestamps[k].append(v)
            page_filled = page["filled"]
            for k, v in event.get("filled", {}).items():
                page_filled[k].append(v)
    return list(event_pages.values())


def bulk_datum_to_datum_page(bulk_datum: dict) -> DatumPage:
    """
    Transform one BulkDatum into one DatumPage.

    Note: There is only one known usage of BulkDatum "in the wild", and the
    BulkDatum layout has been deprecated in favor of DatumPage.
    """
    datum_page = DatumPage(
        datum_id=bulk_datum["datum_ids"],
        resource=bulk_datum["resource"],
        datum_kwargs=_transpose_list_of_dicts(bulk_datum["datum_kwarg_list"]),
    )
    return datum_page


def _transpose_list_of_dicts(list_of_dicts: list) -> dict:
    "Transform list-of-dicts into dict-of-lists (i.e. DataFrame-like)."
    dict_of_lists = defaultdict(list)
    for row in list_of_dicts:
        for k, v in row.items():
            dict_of_lists[k].append(v)
    return dict(dict_of_lists)


def _transpose_dict_of_lists(dict_of_lists: dict) -> list:
    "Transform dict-of-lists (i.e. DataFrame-like) into list-of-dicts."
    list_of_dicts = []
    keys = list(dict_of_lists)
    for row in zip(*(dict_of_lists[k] for k in keys)):
        list_of_dicts.append(dict(zip(keys, row)))
    return list_of_dicts


def verify_filled(event_page: dict) -> None:
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


def sanitize_doc(doc: dict) -> dict:
    """Return a copy with any numpy objects converted to built-in Python types.

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
    """
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
    @no_type_check
    def default(self, obj: object) -> Any:
        try:
            import dask.array

            if isinstance(obj, dask.array.Array):
                obj = numpy.asarray(obj)
        except ImportError:
            pass
        if isinstance(obj, (numpy.generic, numpy.ndarray)):
            if numpy.isscalar(obj):
                return obj.item()
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
