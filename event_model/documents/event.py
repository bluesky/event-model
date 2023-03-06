from typing import Any, Dict, Union

from ._type_wrapper import Field, Annotated, Optional
from typing import TypedDict


class Event(TypedDict):
    """Document to record a quanta of collected data"""

    filled: Optional[
        Annotated[
            Dict[str, Union[bool, str]],
            Field(
                description="Mapping each of the keys of externally-stored data to the boolean False, "
                "indicating that the data has not been loaded, or to foreign keys (moved here from 'data' "
                "when the data was loaded)"
            ),
        ]
    ]

    data: Annotated[Dict[str, Any], Field(description="The actual measurement data")]
    timestamps: Annotated[
        Dict[str, Any],
        Field(description="The timestamps of the individual measurement data"),
    ]
    descriptor: Annotated[
        str, Field(description="UID of the EventDescriptor to which this Event belongs")
    ]
    seq_num: Annotated[
        int,
        Field(
            description="Sequence number to identify the location of this Event in the Event stream",
        ),
    ]
    time: Annotated[
        float,
        Field(
            description="The event time. This maybe different than the timestamps on each of the data entries.",
        ),
    ]
    uid: Annotated[str, Field(description="Globally unique identifier for this Event")]
