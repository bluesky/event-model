===================
 API Documentation
===================

Schemas and Names
=================

The ``event-model`` Python package contains tooling for composing, validating,
and transforming documents in the model.

.. autoclass:: event_model.DocumentNames
   :members:
   :undoc-members:

There are two dictionaries, :data:`event_model.schemas` and
:data:`event_model.schema_validators`, which are keyed on the members of the
:class:`event_model.DocumentNames` enum and which are mapped, respectively, to
a schema and an associated :class:`jsonschema.IValidator`.


Routers
=======


.. autoclass:: event_model.RunRouter
   :members:
   :undoc-members:

.. autoclass:: event_model.SingleRunDocumentRouter
   :members:
   :undoc-members:

.. autoclass:: event_model.DocumentRouter
   :members:
   :undoc-members:

.. autoclass:: event_model.Filler
   :members:

.. autoclass:: event_model.NoFiller
   :members:

.. autofunction:: event_model.register_coercion

.. autofunction:: event_model.as_is


.. autofunction:: event_model.force_numpy


Document Minting
================

To use these functions start with :func:`.compose_run` which will
return a :obj:`.ComposeRunBundle`.

.. autofunction:: event_model.compose_run

.. autoclass:: event_model.ComposeRunBundle

.. autofunction:: event_model.compose_descriptor

.. autoclass:: event_model.ComposeDescriptorBundle

.. autofunction:: event_model.compose_event

.. autofunction:: event_model.compose_event_page

.. autofunction:: event_model.compose_resource

.. autoclass:: event_model.ComposeResourceBundle

.. autofunction:: event_model.compose_datum

.. autofunction:: event_model.compose_datum_page


.. autofunction:: event_model.compose_stop


Document Munging
================


.. autofunction:: event_model.pack_event_page

.. autofunction:: event_model.unpack_event_page

.. autofunction:: event_model.pack_datum_page

.. autofunction:: event_model.unpack_datum_page

.. autofunction:: event_model.sanitize_doc

.. autofunction:: event_model.verify_filled

.. autoclass:: event_model.NumpyEncoder
   :members:
