from typing_extensions import Annotated

from event_model.generate.type_wrapper import BaseModel, Field


class StreamRange(BaseModel):
    """The parameters required to describe a sequence of incrementing integers"""

    start: Annotated[
        int,
        Field(description="First number in the range"),
    ]
    stop: Annotated[
        int,
        Field(description="Last number in the range is less than this number"),
    ]


class StreamDatum(BaseModel):
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