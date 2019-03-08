***************
Release History
***************

v1.8.1 (2019-03-08)
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
