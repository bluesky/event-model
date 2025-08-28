***************
Release History
***************

v1.23.1 (2025-08-28)
====================

Changed
-------

* Update link in docs.
* Removed duplicate `uid` key erroneously included.
* Restored missing `__init__.py` from tests module.
* Added `Document` union type.

v1.23.0 (2025-06-11)
====================

Changed
-------

* Convert from BaseModel to jsonschema + TypedDict
* Add py.typed file to package
* Fix EPICS enum signals support
* Pin pydantic to <2.11, newer versions not supported on Python 3.8
* Allow `None` in descriptor shape.

v1.22.3 (2025-01-07)
====================

Changed
-------
* Updated from copier template.

v1.22.2 (2025-01-07)
====================

Changed
-------
* Fixed linkcheck by updating copier.
* Update README.md.
* Added tango `RDS` alarm parameters and EPICS v4 `hysteresis`.

v1.22.1 (2024-10-25)
====================

Changed
-------
* Adopted the Diamond Light Source copier template.
* Added `types-requests` as a dependency to fix linting.
* Made `units` and `precision` optional again after issues in ophyd and ophyd-async.

v1.22.0 (2024-10-02)
====================

Changed
-------
* Added a `NX_class` field as a `PerObjectHint` to the `EventDescriptor` document.
* Removed `Optional` on `DataKey` fields `precision`, ``choices``, and `units`. These fields can still be left out of the `DataKey`, but if provided they cannot be `None`.
* Added a `Limits` type for storing EPICS limit data and a `NotRequired[Limits]` `limits` field to `DataKey`.
* Added a `NotRequired` `dtype_numpy` field to `DataKey` for specifying the numpy dtype of data.
* Bumped from `jsonschema 3.*` to `jsonschema 4.*`.
* Added all documents to `event_model` namespace.

v1.21.0 (2024-05-21)
====================

Changed
-------

* Update schemas for `StreamResource` and `StreamDatum`: introduced `uri`, `parameters`, and
  `mimetype` fields; removed `path_semantics` and `resource_kwargs`.
* Bump `peaceiris/actions-gh-pages` from 3.9.3 to 4.0.0.

Fixed
-----

* DOC: Fix the switcher URL.

v1.20.0 (2024-03-28)
====================

Changed
-------

- Bumped from `pydantic 1.*` to `pydantic 2.*`

Fixed
-----

- Bug in `rechunk_event_pages` affecting Events with an empty `filled` key

v1.19.9 (2023-11-22)
====================

Changed
-------

* Dropped support for `jsonschema 2.*`.

v1.19.2 (2023-02-24)
====================

Changed
-------

* Fix warnings about distutils being deprecated in Python>=3.10

v1.19.1 (2022-12-09)
====================

This release fixes compatibility with Python 3.7, which was inadvertently
broken in the previous release, 1.19.0.

v1.19.0 (2022-11-03)
====================

Added
-----

* Add two experimental new document types: a :std:ref:`stream_resource` that manages an
  unknown number of contiguous :std:ref:`stream_datum`, with the potential for multiple
  streams. This is especially relevant when the data is expected to be ragged
  or has no pre-determined shape (number of rows).

Changed
-------

* Added ``object_name`` to Event Descriptor schema. The RunEngine has been
  adding this for many years. This change merely documents the status quo.
* Use ``importlib`` instead of ``__version__`` to implement logic conditional
  on jsonschema version.

v1.18.0 (2022-08-05)
====================

Added
-----

* GitHub workflow to publish releases on PyPI

Changed
-------

* Fix for databroker.v0 API
* Fix versioneer compatibility with py311

v1.17.2 (2021-06-21)
====================

Added
-----

* Event, Datum Page were added to TOC

Changed
-------

* The ``start document`` parameter to :func:`~event_model.compose_resource` is now optional
* :meth:`~event_model.RunRouter.descriptor` has been modified to record descriptor document
  id before executing callbacks

v1.17.1 (2021-01-29)
====================

Added
-----

* Any object that implements ``__array__`` is accepted by the schema validators
  as any array. This enables the validator to tolerate numpy-like variants that
  are not literal numpy arrays, such as dask, sparse, or cupy arrays.

v1.17.0 (2020-12-17)
====================

Added
-----

* Added ``data_session`` and ``data_groups`` to Run Start document schema.

Changed
-------

* The package requirements have been relaxed to accept jsonschema versions 2 or
  3. Both are supported.

v1.16.1 (2020-10-15)
====================

Added
-----

* The "projections" schema in the Run Start document has been enhanced to accept "configuration"
  locaitons intended to link to fields that are in Event stream Configuration See :ref:`projections`.
  Additionally, validation of projections was enhanced.
* The method :class:`~event_model.RunRouter.start` was enhanced to check for repeated runs with
  colliding uids, raising an ValueError when this occurs.

v1.16.0 (2020-09-03)
====================

Added
-----

* The schema for Run Start documents now includes an optional "projections"
  key. See :ref:`projections`.
* Added the method :class:`~event_model.DocumentRouter.emit` and an ``emit``
  parameter to :class:`~event_model.DocumentRouter` to support chaining them.
* The :class:`~event_model.Filler` now provides public methods for clearing its caches,
  :meth:`~event_model.Filler.clear_handler_cache` and
  :meth:`~event_model.Filler.clear_document_caches`.
* The method :meth:`~event_model.Filler.deregister_handler` returns the handler
  that it has deregistered.
* The ``filler_state`` attribute of :class:`~event_model.Filler` now includes
  attributes ``resource`` and ``datum`` which may be used by coercion functions
  to work around incorrect ``shape`` metadata.

Changed
-------

* The function :func:`~event_model.register_coercion` replaces
  the misspelled :func:`~event_model.register_coersion`, which is retained as
  an alias for backward-compatibility.

v1.16.0 (2020-09-03)
====================

Added
-----

* The schema for Run Start documents now includes an optional "projections"
  key. See :ref:`projections`.
* Added the method :class:`~event_model.DocumentRouter.emit` and an ``emit``
  parameter to :class:`~event_model.DocumentRouter` to support chaining them.
* The :class:`~event_model.Filler` now provides public methods for clearing its caches,
  :meth:`~event_model.Filler.clear_handler_cache` and
  :meth:`~event_model.Filler.clear_document_caches`.
* The method :meth:`~event_model.Filler.deregister_handler` returns the handler
  that it has deregistered.
* The ``filler_state`` attribute of :class:`~event_model.Filler` now includes
  attributes ``resource`` and ``datum`` which may be used by coercion functions
  to work around incorrect ``shape`` metadata.

Changed
-------

* The function :func:`~event_model.register_coercion` replaces
  the misspelled :func:`~event_model.register_coersion`, which is retained as
  an alias for backward-compatibility.

v1.15.2 (2020-06-12)
====================

Added
-----

* Various documentation additions.
* ``jsonschema 2.x`` compatibility.
* Better naming for handler subclasses.


v1.15.1 (2020-05-01)
====================

Fixed
-----

* A bug was fixed in :class:`~event_model.RunRouter` which caused descriptor
  documents to be sent to subfactory callback start methods.


v1.15.0 (2020-04-27)
====================

Fixed
-----

* In the data model documentation an erroneous link to the RunStart schema
  was corrected to a link to the EventDescriptor schema.

Changed
-------

* :class:`~event_model.SingleRunDocumentRouter` was added with convenience
  methods for getting the start document, the descriptor document for an event
  document, and the stream name for an event document.
* In v1.14.0, :class:`~event_model.RunRouter` was changed to pass the
  RunStart document directly to its callbacks. To smooth the transition, any
  ``Exception`` raised by the callbacks was squashed and a warning printed. With
  v1.15.0 these Exceptions are allowed to propagate. The warning is still
  printed.


v1.14.1 (2020-04-06)
====================

Fixed
-----

* In v1.13.0, the :class:`~event_model.Filler` object was unintentionally made
  un-pickleable. It can now be pickled.
* For validation purposes, we accept numpy arrays as "array"-like.


v1.14.0 (2020-03-11)
====================

Fixed
-----

* Let :func:`~event_model.register_coersion` tolerate duplicate registration of
  the same coersion as the long the duplicate is identical with the original
  (i.e. ``func is original_func``). This is now consistent with how handler
  registration works.
* Fix a critical typo in an error message codepath in
  :func:`~event_model.register_coersion`.

Changed
-------

* The :class:`~event_model.RunRouter` hands RunStart documents to its factory
  functions so they can decide which if any callbacks to subscribe for that
  run. Formerly, the :class:`~event_model.RunRouter` left it up to the factory
  functions to pass the RunStart document through to any callbacks the factory
  function returned. Now, the :class:`~event_model.RunRouter` passes the
  RunStart document to the callbacks directly, removing that responsibility
  from the factory.  To smooth this transition, it does so inside a
  ``try...except`` block and warns if any ``Exception`` is raised. This is a best
  effort at backward-compatibility with factories that are currently passing
  the RunStart document in, though it may not work in every case depending on
  the details of the callback. Likewise for subfactories: the callbacks that
  they return will be given the RunStart document and the relevant
  EventDescriptor document inside a ``try...except`` block.

v1.13.3 (2020-03-05)
====================

Fixed
-----

* Make :func:`~event_model.unpack_event_page` tolerant of Event Pages with
  empty ``data``.

Changed
-------

* Raise a more specific error when :class:`~event_model.Filler` encounters
  an error due to a malformed document.

See the
`GitHub v1.13.3 milestone <https://github.com/bluesky/event-model/milestone/6>`_
for a complete list of changes in this release.

v1.13.2 (2020-01-31)
====================

Fixed
-----

A bug in the new dispatch logic in :class:`~event_model.DocumentRouter`
introduced in v1.13.1 caused the dispatcher to sometimes return
``NotImplemented``. Now it always falls back to returning the original document
if the subclass returns ``None`` or ``NotImplemented``.

v1.13.1 (2020-01-28)
====================

Changed
-------

* The :class:`~event_model.DocumentRouter` converts and routes Event and
  EventPage documents correctly if either one or both of the methods
  ``event`` or ``event_page`` is overridden in the subclass. Likewise for Datum
  and DatumPage and the methods ``datum`` and ``datum_page``. The base class
  implementations all document-type methods now return the Python built-in
  sentinel ``NotImplemented`` (not to be confused with the exception
  ``NotImplementedError``).
* This retry-with-backoff loop in :class:`~event_model.Filler` is now applied
  to handler instantiation as well as handler calls. Either can involve I/O
  with a filesystem that may lag slightly behind the availability of the
  documents.

v1.13.0 (2020-01-21)
====================

Added
-----

* The :class:`~event_model.Filler` accepts an optional parameter ``coerce`` that
  can be used to change the behavior of the handlers. This is useful for
  forcing the filled data to be an in-memory numpy array or a dask array, for
  example. The options accepted by ``coerce`` can be configured at runtime
  using the new function :func:`~event_model.register_coersion`. The coersions
  registered by default are :func:`~event_model.as_is` and
  :func:`~event_model.force_numpy`.
* The :class:`~event_model.NoFiller` has been added. It has the same interface
  as :class:`~event_model.Filler` but it merely *validates* the filling-related
  documents rather than actually filling in the data. This is useful if the
  filling may be done later as a delayed computation but we want to know
  immediately that we have all the information we need to perform that
  computation.
* It is sometimes convenient to make an instance of
  :class:`~event_model.Filler` based on an existing instance but perhaps
  setting some options differently. The new method
  :meth:`~event_model.Filler.clone` takes all the same parameters as Filler
  instantiation. If called with no arguments, it will make a "clone" with all
  the same options. Pass in arguments to override certain options.

Changed
-------

* The :class:`~event_model.NumpyEncoder` special-cases dask arrays.
* Several error messages have been made more specific and useful.

Deprecated
----------

* Field-level filtering in :class:`~event_model.Filler` via the parameters
  ``include`` and ``exclude`` is deprecated.

Internal Changes
----------------

* The code in :class:`~event_model.DocumentRouter` that dispatches based on
  document type has been factored out of ``__call__`` into a new internal
  method, ``_dispatch``, which makes it easier for subclasses to modify
  ``__call__`` but reuse the dispatch logic.

See the
`GitHub v1.13.0 milestone <https://github.com/bluesky/event-model/milestone/3>`_
for a complete list of changes in this release.

v1.12.0 (2019-10-11)
====================

Added
-----

* The :class:`~event_model.RunRouter` can now "fill" documents that reference
  externally stored data. It accepts an optional ``handler_registry`` and
  ``root_map`` which it uses to create instances of
  :class:`~event_model.Filler` internally. The default behavior of
  :class:`~event_model.RunRouter` has not changed because it defaults to
  ``handler_registry={}`` and ``fill_or_fail=False``, meaning that any external
  reference not found in ``handler_registry`` will be passed through unfilled.
  For advanced customizations---such as custom cache management---use the
  parameter ``filler_class`` to specifiy an API-compatible alternative to
  :class:`~event_model.Filler`.

Changed
-------

* The ``handler_registry`` attribute of :class:`~event_model.Filler` is now a
  read-only view. It cannot be directly mutated. Instead, use the new methods
  :meth:`~event_model.Filler.register_handler` and
  :meth:`~event_model.Filler.deregister_handler`.

Fixed
-----

* Fix cache management in :class:`~event_model.Filler` such that registering a
  new handler for a given spec clears all cached instances of the previously
  registered handler.
* Fix the validation feature in :class:`~event_model.DocumentRouter`, which
  previously raised an error if used.

v1.11.2 (2019-09-03)
====================

Fixed
-----

* Include ``requirements.txt`` in source distribution.
* When ``UnresolveableForeignKeyError`` is raised, it always includes a ``key``
  attribute with the key in question.

v1.11.1 (2019-08-09)
====================

Fixed
-----

* Fix some inconsistent behavior in the :class:`~event_model.Filler` ``inplace``
  parameter, and test it better.

v1.11.0 (2019-06-27)
====================

Added
-----

* Added new optional parameter ``inplace`` to :class:`~event_model.Filler`.
* Added new methods :meth:`~event_model.Filler.fill_event` and
  :meth:`~event_model.Filler.fill_event_page`.
* Added :func:`~event_model.rechunk_event_pages`.

Fixed
-----

* Consult the Event Descriptor document to infer which columns need to be
  filled if there is no explicit ``'filled'`` key in the Event document.

v1.10.0 (2019-05-24)
====================

This release requires ``jsonschema>3``. Previous releases required
``jsonschema<3``.

Added
-----
* Added :data:`~event_model.schema_validators` using the new interface in
  jsonschema 3.0.

Fixes
-----
* The counters in ``num_events`` in the RunStop document were off by one.

v1.9.0 (2019-05-01)
===================

Added
-----
* Add experimental :class:`~event_model.RunRouter`.

Fixes
-----
* :func:`~event_model.unpack_datum_page` errored when ``datum_kwargs`` were
  empty.
* Fill EventPages in place, as Events are filled in place.
* Do not assume Events and EventPages have a ``filled`` key; it is optional.

v1.8.3 (2019-03-28)
===================

Fixes
-----
* Add ``'configuration'`` to :ref:`EventDescriptor <descriptor>` schema.
* Fix path semantics and be robust against empty ``'filled'``.
* Fix sequence numbers in :func:`~event_model.compose_descriptor`.
* Fix a typo which made ``'num_events'`` always empty.


v1.8.2 (2019-03-08)
===================

Fix setup.py meta-data to include ``python_requires``.  This prevents
the wheels from being installed on python < 3.6.


v1.8.0 (2019-03-05)
===================

Added
-----
* This documentation!
* Schemas for :ref:`EventPage <event_page>` and :ref:`DatumPage <datum_page>`
* :class:`~event_model.DocumentRouter`, a useful utility adapted from bluesky's
  :class:`CallbackBase`
* :class:`~event_model.Filler`
* :func:`~event_model.verify_filled`
* :func:`~event_model.sanitize_doc` and :class:`~event_model.NumpyEncoder`

v1.7.0 (2019-01-03)
===================

Added
-----

* The DataKey in an EventDescriptors may contain a 'dims' key, providing names
  for each dimension of the data.
* Convenience functions for composing valid documents have been added. These
  are experimental and may change in a future release in a
  non-backward-compatible way.
