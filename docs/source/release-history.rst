***************
Release History
***************

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
