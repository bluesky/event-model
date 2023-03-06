from typing import (
    Any,
    Dict,
    List,
    TYPE_CHECKING,
)

from ._type_wrapper import Field, Annotated, Optional
from typing import Literal
from typing_extensions import TypedDict


class DataKey(TypedDict):
    external: Optional[
        Annotated[
            str,
            Field(
                description="Where the data is stored if it is stored external to the events.",
                regex=r"^[A-Z]+:?",
            ),
        ]
    ]
    dims: Optional[
        Annotated[
            List[str],
            Field(
                description="The names for dimensions of the data. Null or empty list if scalar data",
            ),
        ]
    ]
    object_name: Optional[
        Annotated[
            str,
            Field(description="The name of the object this key was pulled from."),
        ]
    ]

    dtype: Annotated[
        Literal["string", "number", "array", "boolean", "integer"],
        Field(description="The type of the data in the event."),
    ]

    shape: Annotated[
        List[int],
        Field(description="The shape of the data.  Empty list indicates scalar data."),
    ]
    source: Annotated[
        str, Field(description="The source (ex piece of hardware) of the data.")
    ]


if TYPE_CHECKING:
    ObjectHints = Any
else:

    class ObjectHints(TypedDict):
        __root__: Annotated[Any, Field(title="Object Hints")]


class PerObjectHint(TypedDict):
    fields: Optional[
        Annotated[
            List[str],
            Field(description="The 'interesting' data keys for this device."),
        ]
    ]


class Configuration(TypedDict):
    data: Optional[
        Annotated[Dict[str, Any], Field(description="The actual measurement data")]
    ]
    timestamps: Optional[
        Annotated[
            Dict[str, Any],
            Field(description="The timestamps of the individual measurement data"),
        ]
    ]
    data_keys: Optional[
        Annotated[
            Dict[str, DataKey],
            Field(
                description="This describes the data stored alongside it in this configuration object."
            ),
        ]
    ]


class EventDescriptor(TypedDict):
    """Document to describe the data captured in the associated event documents"""

    hints: Optional[ObjectHints]
    object_keys: Optional[
        Annotated[
            Dict[str, Any],
            Field(
                description="Maps a Device/Signal name to the names of the entries it produces in data_keys.",
            ),
        ]
    ]
    name: Optional[
        Annotated[
            str,
            Field(
                description="A human-friendly name for this data stream, such as 'primary' or 'baseline'.",
            ),
        ]
    ]
    configuration: Optional[
        Annotated[
            Dict[str, Configuration],
            Field(
                description="Readings of configurational fields necessary for interpreting data in the Events.",
            ),
        ]
    ]

    data_keys: Annotated[
        Dict[str, DataKey],
        Field(
            description="This describes the data in the Event Documents.",
            title="data_keys",
        ),
    ]
    uid: Annotated[
        str,
        Field(description="Globally unique ID for this event descriptor.", title="uid"),
    ]
    run_start: Annotated[
        str, Field(description="Globally unique ID of this run's 'start' document.")
    ]
    time: Annotated[
        float, Field(description="Creation time of the document as unix epoch time.")
    ]
