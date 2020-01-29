***************
Release History
***************

v1.13.1 (2019-01-28)
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

v1.13.0 (2019-01-21)
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
