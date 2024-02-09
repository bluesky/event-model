from typing import Tuple, Type, Union

# flake8: noqa
from event_model.documents.datum import Datum
from event_model.documents.datum_page import DatumPage
from event_model.documents.event import Event
from event_model.documents.event_descriptor import EventDescriptor
from event_model.documents.event_page import EventPage
from event_model.documents.resource import Resource
from event_model.documents.run_start import RunStart
from event_model.documents.run_stop import RunStop
from event_model.documents.stream_datum import StreamDatum
from event_model.documents.stream_resource import StreamResource

DocumentType = Union[
    Type[Datum],
    Type[DatumPage],
    Type[Event],
    Type[EventDescriptor],
    Type[EventPage],
    Type[Resource],
    Type[RunStart],
    Type[RunStop],
    Type[StreamDatum],
    Type[StreamResource],
]

ALL_DOCUMENTS: Tuple[DocumentType, ...] = (
    Datum,
    DatumPage,
    Event,
    EventDescriptor,
    EventPage,
    Resource,
    RunStart,
    RunStop,
    StreamDatum,
    StreamResource,
)


__all__ = [
    "Datum",
    "DatumPage",
    "Event",
    "EventDescriptor",
    "EventPage",
    "Resource",
    "RunStart",
    "RunStop",
    "StreamDatum",
    "StreamResource",
    "DocumentType",
]
