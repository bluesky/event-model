***************
Release History
***************

Unreleased
==========

Added
-----
* This documentation!
* Schemas for :ref:`EventPage <event_page>` and :ref:`DatumPage <datum_page>`
* :class:`~event_model.DocumentRouter`, a useful utility adapted from bluesky's
  :class:`CallbackBase`
* :class:`~event_model.Filler`
* :func:`~event_model.verify_filled`
* :func:`~event_model.sanitize_doc`

v1.7.0 (3 January 2019)
=======================

Added
-----

* The DataKey in an EventDescriptors may contain a 'dims' key, providing names
  for each dimension of the data.
* Convenience functions for composing valid documents have been added. These
  are experimental and may change in a future release in a
  non-backward-compatible way.
