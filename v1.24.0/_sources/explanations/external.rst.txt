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
             'temperature': 5.0},
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

The presence of the key ``'external'`` indicates that the Events' ``'image'``
contains a reference to an asset outside the documents. (The value of
that key is not currently used by any part of the system; only its existence is
checked for. The value may be used in the futue as a hook for integration with
outside systems.)

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
example it might be an image or stack of images that were taken at a
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
    'run_start': '10bf6945-4afd-43ca-af36-6ad8f3540bcd'}

The ``resource_path`` is a relative path, all of which is semantic and should
usually not change during the lifecycle of this asset. The ``root`` is more
context-dependent (depending on what system you are accessing the data from)
and subject to change (if the data is moved over time).

The ``spec`` gives us a hint about the format of this asset, whether it be a
file, multiple files, or something more specialized. The ``resource_kwargs``
provide any additional parameters for reading it.

 .. code:: python

   # 'Stream Resource' document
   {'uid': 'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c',
    'mimetype': 'application/x-hdf5',
    'uri': 'file://localhost/{path}/GPFS/DATA/Andor/2020/01/03/8ff08ff9.h5',
    'parameters': {'frame_per_point': 10},
    'run_start': '10bf6945-4afd-43ca-af36-6ad8f3540bcd'}

The ``uri`` specifies the location of the data. It may be a path on the local
filesystem, `file://localhost/{path}`, a path on a shared filesystem
`file://{host}/{path}`, to be remapped at read time via local mount config,
or a non-file-based resource like `s3://...`. The `{path}` part of the `uri`
is typically a relative path, all of which is semantic and should usually not
change during the lifecycle of this asset.

The ``mimetype`` is a recognized standard way to specify the I/O procedures to
read the asset. It gives us a hint about the format of this asset, whether it
be a file, multiple files, or something more specialized. We support standard
mimetypes, such as `image/tiff`, as well as custom ones, e.g.
`application/x-hdf5-smwr-slice`. The ``parameters`` provide any additional
parameters for reading the asset.

Handlers
========

In bluesky/databroker, a "handler" is a reader with a special interface. It
accepts a Resource document and a Datum document and in exchange returns the
pertinent data.

Handler Interface
-----------------

A 'handler class' may be any callable with the signature::

    handler_class(full_path, **resource_kwargs)

It is expected to return an object, a 'handler instance', which is also
callable and has the following signature::

    handler_instance(**datum_kwargs)

As the names 'handler class' and 'handler instance' suggest, this is
typically implemented using a class that implements ``__init__`` and
``__call__``, with the respective signatures.

.. code:: python

   class MyHandler:
       def __init__(self, path, **resource_kwargs):
           # Consume the path information and the 'resource_kwargs' from the
           # Resource. Typically stashes some state and/or opens file(s).
           ...

       def __call__(self, **datum_kwargs):
           # Consumes the 'datum_kwargs' from the datum and uses them to
           # locate a specific unit (slice, chunk, or what you will...) of
           # data and return it.
           ...
           return some_array_like

But in general it may be any callable-that-returns-a-callable.

.. code:: python

   def handler(path, **resource_kwargs):
       def f(**datum_kwargs):
           return some_array_like
       return f

A handler may also implement the instance method ``get_file_list()``. This
presumes that the data in question comes from a filesystem, which may not
always be the case, which is why this method is optional.

A handler should implement ``close()`` if it caches any file handles, network
connections or other system resources. The lifecycle of a handler is an
implementation detail left up to the application. Below, we comment on how
:class:`~event_model.Filler` and :class:`~event_model.RunRouter` make it easier
to reuse handler instances and clean them up at the proper time.

Handler Discovery
-----------------

To discover all the handlers installed in an environment, use

.. code:: python

   import databroker.core
   handler_registry = databroker.core.discover_handlers()

The result, ``handler_registry``, is a dict mapping specs to handler classes.
It uses an efficient mechanism, described later, for searching the installed
packages for handlers. Thus, its contents will depend on which packages you
have installed. In this case, we have installed the Python package
``area-detector-handlers`` which includes several handlers for reading the
files output by area detectors.

.. code:: none

   {'AD_CBF': <class 'area_detector_handlers.handlers.PilatusCBFHandler'>,
    'AD_HDF5': <class 'area_detector_handlers.handlers.AreaDetectorHDF5Handler'>,
    'AD_HDF5_SWMR': <class 'area_detector_handlers.handlers.AreaDetectorHDF5SWMRHandler'>,
    'AD_HDF5_SWMR_TS': <class 'area_detector_handlers.handlers.AreaDetectorHDF5SWMRTimestampHandler'>,
    'AD_HDF5_TS': <class 'area_detector_handlers.handlers.AreaDetectorHDF5TimestampHandler'>,
    'AD_SPE': <class 'area_detector_handlers.handlers.AreaDetectorSPEHandler'>,
    'AD_TIFF': <class 'area_detector_handlers.handlers.AreaDetectorTiffHandler'>,
    'XSP3': <class 'area_detector_handlers._xspress3.Xspress3HDF5Handler'>,
    'XSP3_FLY': <class 'area_detector_handlers._xspress3.BulkXSPRESS'>}

To hook into this discovery mechanism, see the section :ref:`handler_packaging`
below.

Filling
=======

It is rarely necessary to create handlers directly. The
:class:`~event_model.Filler` object is designed to consume documents from a
Run, determine which data is external, and create handlers as needed to access
the external data, and "fill" that external in, moving the ``datum_id`` to a
separate field.

Before filling:

.. code:: python

   # 'event' document before filling
   {'data': {'image': 'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c/5',
             'temperature': 5.0},
    'descriptor': '219310e0-faa0-4990-84a0-95b508d4ae35',
    'filled': {'image': False}
    ...}

After filling:

.. code:: python

   # 'event' document after filling
   {'data': {'image':, [[...]]  # array-like object
             'temperature': 5.0},
    'descriptor': '219310e0-faa0-4990-84a0-95b508d4ae35',
    'filled': {'image': 'aa10035d-1d2b-41d9-97e6-03e3fe62fa6c/5'}
    ...}

Notice that the ``datum_id`` is still in the document; it has been moved out of
the way into the ``'filled'`` mapping. The ``'filled'`` mapping is a way to
track which if any keys on a document "in flight" have already been filled.
It is allowable for an Event or EventPage to be _partially_ filled, where the
``'data'`` mapping contains a mixture of filled and not-yet-filled items.
Fields that are not externally-stored (such as ``'temperature'`` in our
example) do not appear in the ``'filled'`` mapping. Thus, the keys in the
``'filled'`` mapping are subset of the keys in ``'data'``.

A Filler takes in a ``handler_registry``, such as the one shown in the previous
section.

.. code:: python

   import event_model
   filler = event_model.Filler(handler_registry)

It uses the ``'spec'`` in each Resource document to find a matching
handler class in its registry. If it cannot find a match for a given spec, an
:class:`~event_model.UndefinedAssetSpecification` error is raised.

Resource Management
===================

A primary concern here is resource management. Fillers create and cache
instances of handlers, which in turn may cache instances of file handles,
network connections, or other system resources.
When a Filler is closed with :meth:`~event_model.Filler.close` or used as a
context manager, it releases all its handlers which in turn should close any
resources they have allocated. The caches used by a Filler are injectable: by
default all relevant documents and handler instances are cached until the
Filler is closed, but the Filler can be configured to use any custom cache
object, such as a :class:`cachetools.LRUCache` or
:class:`cachetools.LFUCache`, to receive a prepopulated cache, or to share
caches between Filler instances. This is an implementation detail left entirely
up to the application. See :class:`~event_model.Filler` for details on cache
injection. Here is an example where two Fillers share a global LRU cache:

.. code:: python

   import event_model
   import cachetools

   handler_registry = {...}  # or use databroker.core.discover_handlers()

   handler_cache = cachetools.LRUCache(32)
   f1 = Filler(handler_registry, handler_cache=handler_cache)
   f2 = Filler(handler_registry, handler_cache=handler_cache)

If both fillers are asked for the same Resource, they can share the same
handler instance and any system resources cached therein. When the handler is
evicted from the LRUCache, the Filler will recover gracefully: an instance will
be recreated on demand and put back into the cache.

When streaming data from multiple runs, it is convenient to use the
:class:`~event_model.RunRouter` to manage Filler creation and disposal.
It accepts a ``handler_registry`` and other optional Filler-related arguments.
It uses them to make a separate Filler instance for each Run, which it closes
when it sees the last document from the Run.

.. code:: python

   import event_model
   rr = event_model.RunRouter([...], handler_registry=handler_registry)

See :class:`~event_model.RunRouter` and :class:`~event_model.Filler` for more.

.. _handler_packaging:

Handler Packaging
=================

Packages can use the ``'databroker.handlers'``
`entrypoint <https://packaging.python.org/specifications/entry-points/>`_
to declare that they include some handlers. See for example this excerpt from
the ``setup.py`` in https://github.com/bluesky/area-detector-handlers

.. code:: python

   setup(
       ...
       entry_points={
           "databroker.handlers": [
               "AD_SPE = area_detector_handlers.handlers:AreaDetectorSPEHandler",
               "AD_TIFF = area_detector_handlers.handlers:AreaDetectorTiffHandler",
               "AD_HDF5 = area_detector_handlers.handlers:AreaDetectorHDF5Handler",
               "AD_HDF5_SWMR = area_detector_handlers.handlers:AreaDetectorHDF5SWMRHandler",
               "AD_HDF5_TS = area_detector_handlers.handlers:AreaDetectorHDF5TimestampHandler",
               "AD_HDF5_SWMR_TS = area_detector_handlers.handlers:AreaDetectorHDF5SWMRTimestampHandler",
               "XSP3 = area_detector_handlers.handlers:Xspress3HDF5Handler",
               "AD_CBF = area_detector_handlers.handlers:PilatusCBFHandler",
               "XSP3_FLY = area_detector_handlers.handlers:BulkXSPRESS",
               "IMM = area_detector_handlers.handlers:IMMHandler",
           ]
       },
       ...)

On the left-hand side of the ``=`` is given the spec, matching the ``'spec'``
in the Resource document, and on the right-hand side is given the
``path.to.module:object_name`` of the handler class that can handle that type
of asset.
