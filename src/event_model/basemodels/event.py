from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field


class PartialEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data: Annotated[dict[str, Any], Field(description="The actual measurement data")]
    filled: Annotated[
        dict[str, bool | str],
        Field(
            default_factory=dict,
            description="Mapping each of the keys of externally-stored data to the "
            "boolean False, indicating that the data has not been loaded, or to "
            "foreign keys (moved here from 'data' when the data was loaded)",
        ),
    ]
    time: Annotated[
        float,
        Field(
            description="The event time. This maybe different than the timestamps on "
            "each of the data entries.",
        ),
    ]
    timestamps: Annotated[
        dict[str, Any],
        Field(description="The timestamps of the individual measurement data"),
    ]


class Event(PartialEvent):
    """Document to record a quanta of collected data"""

    model_config = ConfigDict(extra="forbid")

    descriptor: Annotated[
        str, Field(description="UID of the EventDescriptor to which this Event belongs")
    ]
    seq_num: Annotated[
        int,
        Field(
            description="Sequence number to identify the location of this Event in the "
            "Event stream",
        ),
    ]
    uid: Annotated[str, Field(description="Globally unique identifier for this Event")]
