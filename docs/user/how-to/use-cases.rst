============================
Event Model Patterns
============================
When implementing a system with the Event Model for a particular use case (technique, scan type, etc.), many design choices can be made. For example: how many streams you define (through Event Descriptor documents), what events you put into those streams, and what how data points are stored in an event. Here, we present a use case and a potential design, discussing the pros and cons of different options.

To further complicate things, we can consider that a document stream might be be optimized for very different scenarios within the same technique. For example, for the sample scan of the same sample, a document stream might read as the scan as it is being run for the purpose of providing in-scan feedback to a user or beamline analysis tool. For the same scan, a docstream might be serialized into a Mongo database and read back out by analysis and visualiztion tools. In these different uses of data from the same scan, the level of granularity that one puts into an Event Document might be very different. The streaming consumer might require a large number of very small granular events in order to quickly make decisions that affect the course of the scan. On the other hand, MongoDB document retrieval is much more efficient with a small number of larger documents, and a small number of events that each contain data from multiple time steps might be preferrable.


Use Case - Tomography Tiling and MongoDB Serialization
_______________________________________________________
A common pattern in tomography is the concept of "tiling". When tiling, a single location is chosen, a number of scans are taken around an axis to create a volume for the tile, then the sample is moved to take another tile. Post processing tools then stitch the various tiles together, creating a mosaic.

How are these best represented in event model documents? We consider two alternative, based on the relationship between Event and Event Descriptor documents:

- Represent each tile as its own stream, letting the concept of the stream be a natural container.
- Define a single "primary" stream for all sample scan events, then store tile information in fields in each event (either as a separate tile-identifier field, or let analysis tools note tile boundaries with changes in sample position.)

**Multiple Streams**

With multiple streams, we would assign multiple Descriptor documents to represent each tile:


.. code-block:: python

   #  Descriptors - one for each tile

   {"uid": "4fa521e7-fcdc-4a68-9171-d4838d1fc9a2",
   "time": 1600995123.1785965,
   "run_start": "9b5dd575-b556-439a-ba71-357307caebb9",
   . . .
   "name": "tile_0"}

   {"uid": "a82a83b0-5959-4ca3-a7ed-7fb1d0f07994",
   "time": 1600995123.1785965,
   "run_start": "9b5dd575-b556-439a-ba71-357307caebb9",
   . . .
   "name": "tile_1"}

Then each Event would carry data for the tile. Here we show two events for each of the two tile descriptors:

.. code-block:: python

   #  Events

   {"uid": "c4aa6916-3d84-446c-850a-95fb71cee6b6", 
    "time": 1600995123.1808913, 
    "data": { ...}, 
    "timestamps": { ... },
    "seq_num": 1, 
    "descriptor": "4fa521e7-fcdc-4a68-9171-d4838d1fc9a2"}

    {"uid": "c4aa6916-3d84-446c-850a-95fb71cee6b6", 
    "time": 1600995123.1808913, 
    "data": { ...}, 
    "timestamps": { ... },
    "seq_num": 2, 
    "descriptor": "4fa521e7-fcdc-4a68-9171-d4838d1fc9a2"}

    {"uid": "c4aa6916-3d84-446c-850a-95fb71cee6b6", 
    "time": 1600995123.1808913, 
    "data": { ...}, 
    "timestamps": { ... },
    "seq_num": 1, 
    "descriptor": "a82a83b0-5959-4ca3-a7ed-7fb1d0f07994"}

    {"uid": "c4aa6916-3d84-446c-850a-95fb71cee6b6", 
    "time": 1600995123.1808913, 
    "data": { ...}, 
    "timestamps": { ... },
    "seq_num": 2, 
    "descriptor": "a82a83b0-5959-4ca3-a7ed-7fb1d0f07994"}


**Single Stream**

With a single stream, we would create a single descriptor document that all events map to, then each tile would be somehow identified within the Event:

.. code-block:: python

   #  Descriptor - just one for all tiles

   {"uid": "4fa521e7-fcdc-4a68-9171-d4838d1fc9a2",
   "time": 1600995123.1785965,
   "run_start": "9b5dd575-b556-439a-ba71-357307caebb9",
   . . .
   "name": "primary"}

Then each Event would carry data for the tile. Here we show two events for each of the two tile descriptors. Again, we are showing two tiles and two events per tile:

.. code-block:: python

   #  Events - events have 

   {"uid": "c4aa6916-3d84-446c-850a-95fb71cee6b6", 
    "time": 1600995123.1808913, 
    "data": { "tile_id": "0",  ...}, 
    "timestamps": { ... },
    "seq_num": 1, 
    "descriptor": "4fa521e7-fcdc-4a68-9171-d4838d1fc9a2"}

    {"uid": "c4aa6916-3d84-446c-850a-95fb71cee6b6", 
    "time": 1600995123.1808913, 
    "data": { "tile_id": "0",  ...}, 
    "timestamps": { ... },
    "seq_num": 2, 
    "descriptor": "4fa521e7-fcdc-4a68-9171-d4838d1fc9a2"}

    {"uid": "c4aa6916-3d84-446c-850a-95fb71cee6b6", 
    "time": 1600995123.1808913, 
    "data": { "tile_id": "1",  ...}, 
    "timestamps": { ... },
    "seq_num": 3, 
    "descriptor": "4fa521e7-fcdc-4a68-9171-d4838d1fc9a2"}

    {"uid": "c4aa6916-3d84-446c-850a-95fb71cee6b6", 
    "time": 1600995123.1808913, 
    "data": { "tile_id": "1",  ...}, 
    "timestamps": { ... },
    "seq_num": 4, 
    "descriptor": "4fa521e7-fcdc-4a68-9171-d4838d1fc9a2"}

**Summary**
Both layouts carry advantages. The number of tiles can be quite large in tomographic scans, adding a large number of descriptor documents. Hijacking the concept of a stream for the purpose of defining tile boundaries might challenge the intent of streams, but could potentially prove extremely useful for downstream analysis and visulization tools that will require definitions of tile bounaries in their events.

Other Use Cases
____________________________

Do you have other interesting use cases for event model structures? Please contribute them here!
