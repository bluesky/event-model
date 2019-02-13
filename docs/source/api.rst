*****************
API Documentation
*****************

The ``event-model`` Python package contains tooling for composing, validating,
and transforming documents in the model.

.. autoclass:: event_model.DocumentNames
   :members:
   :undoc-members:

.. autoclass:: event_model.DocumentRouter
   :members:
   :undoc-members:

.. autoclass:: event_model.Filler
   :members:

.. autofunction:: event_model.verify_filled

.. autofunction:: event_model.pack_event_page

.. autofunction:: event_model.unpack_event_page

.. autofunction:: event_model.pack_datum_page

.. autofunction:: event_model.unpack_datum_page

.. autofunction:: event_model.sanitize_doc

.. autoclass:: event_model.NumpyEncoder
   :members:

.. autofunction:: event_model.compose_run

.. autofunction:: event_model.compose_descriptor

.. autofunction:: event_model.compose_resource

.. autofunction:: event_model.compose_datum

.. autofunction:: event_model.compose_datum_page

.. autofunction:: event_model.compose_event

.. autofunction:: event_model.compose_event_page

.. autofunction:: event_model.compose_stop
