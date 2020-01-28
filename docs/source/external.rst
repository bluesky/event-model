***************
External Assets
***************

The data model allows an Event document to contain a mixture of literal values
and references to externally-stored values. Scalar values and very small arrays
are typically placed directly in the document; large arrays such as from area
detectors are typically stored externally.

This design keeps the documents of reasonable size---suitable for storing in
MongoDB or viewing directly as JSON text---and it allows large assets to be
loaded only when needed.

Suppose we have this Event

.. code:: python

   # 'event' document
   {'data': {'image': 'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c/5',
             'temperature': 5.0}
    'descriptor': '219310e0-faa0-4990-84a0-95b508d4ae35',
    ...}

where the other fields in the Event have been omitted (``...``) for brevity.
We can tell that the value of ``'image'`` is a placeholder, a foreign key
referencing some array yet to be retrieved, by consulting the Event Descriptor
referenced by this Event.

(Of course, a human may be able to *guess* that the value of image looks like a
placeholder, but that wouldn't help a program.)

Here is the Event Descriptor that goes with this Event; we can tell that
because its ``'uid'`` matches the Event's ``'descriptor'`` field.

.. code:: python

   # 'descriptor' document
   {'uid': '219310e0-faa0-4990-84a0-95b508d4ae35',
    'data_keys':
       {'image':
           {'source': '...',
            'shape': [512, 512],
            'dtype': 'array',
            'external': '...'},
        'temperature': 
           {'source': '...',
            'shape': [],
            'dtype': 'number'}}
    ...}

The precence of the key ``'external'`` (regardless of its value) indicates that
the Events' ``'image'`` contains a reference to an asset outside the documents.
