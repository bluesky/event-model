from typing import Tuple, Type, Union

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

Document = Union[
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
]

ALL_BASEMODELS: Tuple[DocumentType, ...] = (
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
