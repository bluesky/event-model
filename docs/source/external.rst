***************
External Assets
***************

The Documents
=============

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

(Of course, a human may be able to *guess* that the value of ``'image'`` looks
like a placeholder, but that wouldn't help a program.)

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

Returning to our Event

.. code:: python

   # 'event' document
   {'data': {'image': 'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c/5',
             'temperature': 5.0}
    'descriptor': '219310e0-faa0-4990-84a0-95b508d4ae35',
    ...}

now that we know that ``'image'`` is external, the value
``'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c/5'`` must be a ``datum_id``,
referencing a Datum document. Here is the matching Datum document. This
document can be used to retrieve some data that belong in our Event. In our
example it might be an image or stack of images images that were taken at a
given temperature during a temperature scan.

.. code:: python

   # 'datum' document
   {'datum_id': 'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c/5',
    'datum_kwargs': {'index': 5},
    'resource': 'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c'}

You will notice that we still do not have a *filepath* anywhere here. It is
common for many Datum documents to point into the same file (e.g. a large HDF5
file) or series of files (e.g. TIFF series).  Rather than store that
information separately and redundantly in each Datum, the Datum documents point
to a Resource document---the last document type we'll need here---which
contains path-related details.

 .. code:: python

   # 'resource' document
   {'uid': 'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c',
    'spec': 'AD_HDF5',
    'root': '/GPFS/DATA/Andor/',
    'resource_path': '2020/01/03/8ff08ff9-a2bf-48c3-8ff3-dcac0f309d7d.h5',
    'resource_kwargs': {'frame_per_point': 10},
    'path_semantics': 'posix',
    'uid': '3b300e6f-b431-4750-a635-5630d15c81a8',
    'run_start': '10bf6945-4afd-43ca-af36-6ad8f3540bcd'}

The ``resource_path`` is a relative path, all of which is semantic and should
usually not change during the lifecycle of this asset. The ``root`` is more
context-dependent (depending on what system you are accessing the data from)
and subject to change (if the data is moved over time).

The ``spec`` gives us a hint as how choose a handler that can read this asset
(whether it be a file, multiple files, or something more specialized).

Handlers
========

TODO
