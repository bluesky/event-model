# generated in `event_model/generate`

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

DocumentType = (
    type[Datum]  # noqa: F405
    | type[DatumPage]  # noqa: F405
    | type[Event]  # noqa: F405
    | type[EventDescriptor]  # noqa: F405
    | type[EventPage]  # noqa: F405
    | type[Resource]  # noqa: F405
    | type[RunStart]  # noqa: F405
    | type[RunStop]  # noqa: F405
    | type[StreamDatum]  # noqa: F405
    | type[StreamResource]  # noqa: F405
)

Document = (
    Datum  # noqa: F405
    | DatumPage  # noqa: F405
    | Event  # noqa: F405
    | EventDescriptor  # noqa: F405
    | EventPage  # noqa: F405
    | Resource  # noqa: F405
    | RunStart  # noqa: F405
    | RunStop  # noqa: F405
    | StreamDatum  # noqa: F405
    | StreamResource  # noqa: F405
)

ALL_DOCUMENTS: tuple[DocumentType, ...] = (
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
