from typing_extensions import Annotated, TypedDict

from .generate.type_wrapper import Field, add_extra_schema


class StreamRange(TypedDict):
    """The parameters required to describe a sequence of incrementing integers"""

    start: Annotated[
        int,
        Field(description="First number in the range"),
    ]
    stop: Annotated[
        int,
        Field(description="Last number in the range is less than this number"),
    ]


STREAM_DATUM_EXTRA_SCHEMA = {"additionalProperties": False}


@add_extra_schema(STREAM_DATUM_EXTRA_SCHEMA)
class StreamDatum(TypedDict):
    """Document to reference a quanta of an externally-stored stream of data."""

    descriptor: Annotated[
        str,
        Field(description="UID of the EventDescriptor to " "which this Datum belongs"),
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
            "formatting being '<stream_resource>/<stream_name>/<block_id>"
        ),
    ]
    seq_nums: Annotated[
        StreamRange,
        Field(
            description="A slice object showing the Event numbers the "
            "resource corresponds to"
        ),
    ]
    indices: Annotated[
        StreamRange,
        Field(
            description="A slice object passed to the StreamResource "
            "handler so it can hand back data and timestamps"
        ),
    ]
