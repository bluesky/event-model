.. _schema_generation:

*****************
Schema Generation
*****************

To allow for python typing of documents, we define them as `TypedDict` in `event_model.documents`.

.. literalinclude:: ../../src/event_model/documents/datum.py
  :language: python

We then use pydantic to convert these python types into the jsonschema in `event_model.schemas`.

After changing any of the documents it's necessary to regenerate the schemas. This can be done by running:

.. code-block:: bash

   python -m event_model.generate

which is a python environment script in a dev install of event-model.

This ensures we can have accurate typing across the bluesky codebase, but also doesn't limit us to python for validating documents.
