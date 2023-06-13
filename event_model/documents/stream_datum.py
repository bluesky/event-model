from typing import Any, Dict

from typing_extensions import Annotated, TypedDict

from .generate.type_wrapper import Field, add_extra_schema

STREAM_DATUM_EXTRA_SCHEMA = {"additionalProperties": False}


@add_extra_schema(STREAM_DATUM_EXTRA_SCHEMA)
class StreamDatum(TypedDict):
    """Document to reference a quanta of an externally-stored stream of data."""

    block_idx: Annotated[
        int,
        Field(
            description="The order in the stream of this block of data. This must "
            "be contiguous for a given stream.",
        ),
    ]
    datum_kwargs: Annotated[
        Dict[str, Any],
        Field(
            description="Arguments to pass to the Handler to retrieve one "
            "quanta of data",
        ),
    ]
    event_count: Annotated[
        int, Field(description="The number of events in this datum.")
    ]
    event_offset: Annotated[
        int,
        Field(
            description="The sequence number of the first event in this block. This "
            "increasing value allows the presence of gaps.",
        ),
    ]
    stream_name: Annotated[
        str,
        Field(
            description="The name of the stream that this Datum is providing a "
            "block of.",
        ),
    ]
    stream_resource: Annotated[
        str,
        Field(
            description="The UID of the Stream Resource to which this Datum belongs."
        ),
    ]
    uid: Annotated[
        str,
        Field(
            description="Globally unique identifier for this Datum. A suggested "
            "formatting being '<stream_resource>/<stream_name>/<block_id>",
        ),
    ]
