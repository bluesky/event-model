# generated in `event_model/generate`

from typing import Tuple, Type, Union

from .datum import *  # noqa: F403
from .datum_page import *  # noqa: F403
from .event import *  # noqa: F403
from .event_descriptor import *  # noqa: F403
from .event_page import *  # noqa: F403
from .resource import *  # noqa: F403
from .run_start import *  # noqa: F403
from .run_stop import *  # noqa: F403
from .stream_datum import *  # noqa: F403
from .stream_resource import *  # noqa: F403

DocumentType = Union[
    Type[Datum],  # noqa: F405,
    Type[DatumPage],  # noqa: F405,
    Type[Event],  # noqa: F405,
    Type[EventDescriptor],  # noqa: F405,
    Type[EventPage],  # noqa: F405,
    Type[Resource],  # noqa: F405,
    Type[RunStart],  # noqa: F405,
    Type[RunStop],  # noqa: F405,
    Type[StreamDatum],  # noqa: F405,
    Type[StreamResource],  # noqa: F405,
]

Document = Union[
    Datum,  # noqa: F405
    DatumPage,  # noqa: F405
    Event,  # noqa: F405
    EventDescriptor,  # noqa: F405
    EventPage,  # noqa: F405
    Resource,  # noqa: F405
    RunStart,  # noqa: F405
    RunStop,  # noqa: F405
    StreamDatum,  # noqa: F405
    StreamResource,  # noqa: F405
]

ALL_DOCUMENTS: Tuple[DocumentType, ...] = (
    Datum,  # noqa: F405
    DatumPage,  # noqa: F405
    Event,  # noqa: F405
    EventDescriptor,  # noqa: F405
    EventPage,  # noqa: F405
    Resource,  # noqa: F405
    RunStart,  # noqa: F405
    RunStop,  # noqa: F405
    StreamDatum,  # noqa: F405
    StreamResource,  # noqa: F405
)
