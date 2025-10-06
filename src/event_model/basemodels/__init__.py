from event_model.basemodels.datum import Datum
from event_model.basemodels.datum_page import DatumPage
from event_model.basemodels.event import Event
from event_model.basemodels.event_descriptor import (
    Dtype,
    EventDescriptor,
    Limits,
    LimitsRange,
)
from event_model.basemodels.event_page import EventPage
from event_model.basemodels.resource import Resource
from event_model.basemodels.run_start import RunStart
from event_model.basemodels.run_stop import RunStop
from event_model.basemodels.stream_datum import StreamDatum
from event_model.basemodels.stream_resource import StreamResource

DocumentType = (
    type[Datum]
    | type[DatumPage]
    | type[Event]
    | type[EventDescriptor]
    | type[EventPage]
    | type[Resource]
    | type[RunStart]
    | type[RunStop]
    | type[StreamDatum]
    | type[StreamResource]
)

Document = (
    Datum
    | DatumPage
    | Event
    | EventDescriptor
    | EventPage
    | Resource
    | RunStart
    | RunStop
    | StreamDatum
    | StreamResource
)

ALL_BASEMODELS: tuple[DocumentType, ...] = (
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
    "Dtype",
    "Event",
    "EventDescriptor",
    "EventPage",
    "Limits",
    "LimitsRange",
    "Resource",
    "RunStart",
    "RunStop",
    "StreamDatum",
    "StreamResource",
    "DocumentType",
]
