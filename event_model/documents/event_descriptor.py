from typing import TYPE_CHECKING, Any, Dict, List

from typing_extensions import Annotated, NotRequired, Literal, TypedDict

from ._type_wrapper import Field


class DataKey(TypedDict):
    external: NotRequired[
        Annotated[
            str,
            Field(
                description="Where the data is stored if it is stored external to the events.",
                regex=r"^[A-Z]+:?",
            ),
        ]
    ]
    dims: NotRequired[
        Annotated[
            List[str],
            Field(
                description="The names for dimensions of the data. Null or empty list if scalar data",
            ),
        ]
    ]
    object_name: NotRequired[
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
    fields: NotRequired[
        Annotated[
            List[str],
            Field(description="The 'interesting' data keys for this device."),
        ]
    ]


class Configuration(TypedDict):
    data: NotRequired[
        Annotated[Dict[str, Any], Field(description="The actual measurement data")]
    ]
    timestamps: NotRequired[
        Annotated[
            Dict[str, Any],
            Field(description="The timestamps of the individual measurement data"),
        ]
    ]
    data_keys: NotRequired[
        Annotated[
            Dict[str, DataKey],
            Field(
                description="This describes the data stored alongside it in this configuration object."
            ),
        ]
    ]


class EventDescriptor(TypedDict):
    """Document to describe the data captured in the associated event documents"""

    hints: NotRequired[ObjectHints]
    object_keys: NotRequired[
        Annotated[
            Dict[str, Any],
            Field(
                description="Maps a Device/Signal name to the names of the entries it produces in data_keys.",
            ),
        ]
    ]
    name: NotRequired[
        Annotated[
            str,
            Field(
                description="A human-friendly name for this data stream, such as 'primary' or 'baseline'.",
            ),
        ]
    ]
    configuration: NotRequired[
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
