3. Made all documents accessible from the top level namespace
=============================================================

Date: 2024-10-03

Status
------

Accepted

Context
-------

We should make all documents importable from the top level event-model namespace. We should be able to `from event_model import Limits` rather than `from event_model.documents import Limits` or `from event_model.documents.event_descriptor import Limits`.


Decision
--------

Accepted

Consequences
------------
Repositories downstream will be able to simplify their imports.
