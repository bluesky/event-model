from typing import Dict, List, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
)
from typing_extensions import Annotated


class DataFrameForFilled(RootModel):
    root: Dict[str, List[Union[bool, str]]] = Field(alias="DataframeForFilled")


class DataFrameForEventPage(RootModel):
    root: Dict[str, List] = Field(alias="Dataframe")


class PartialEventPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data: Annotated[
        DataFrameForEventPage,
        Field(description="The actual measurement data"),
    ]
    filled: Annotated[
        Optional[DataFrameForFilled],
        Field(
            description="Mapping each of the keys of externally-stored data to an "
            "array containing the boolean False, indicating that the data has not "
            "been loaded, or to foreign keys (moved here from 'data' when the data "
            "was loaded)",
            default=None,
        ),
    ]
    timestamps: Annotated[
        DataFrameForEventPage,
        Field(description="The timestamps of the individual measurement data"),
    ]
    time: Annotated[
        List[float],
        Field(
            description="Array of Event times. This maybe different than the "
            "timestamps on each of the data entries"
        ),
    ]


class EventPage(PartialEventPage):
    """Page of documents to record a quanta of collected data"""

    model_config = ConfigDict(extra="forbid")

    descriptor: Annotated[
        str,
        Field(
            description="The UID of the EventDescriptor to which all of the Events in "
            "this page belong",
        ),
    ]

    seq_num: Annotated[
        List[int],
        Field(
            description="Array of sequence numbers to identify the location of each "
            "Event in the Event stream",
        ),
    ]
    uid: Annotated[
        List[str],
        Field(description="Array of globally unique identifiers for each Event"),
    ]
