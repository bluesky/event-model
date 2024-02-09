from typing import Any, Dict, List, Optional

from typing_extensions import Annotated, Literal, NotRequired, TypedDict

from .generate.type_wrapper import Field, add_extra_schema

Dtype = Literal["string", "number", "array", "boolean", "integer"]


class DataKey(TypedDict):
    """Describes the objects in the data property of Event documents"""

    dims: NotRequired[
        Annotated[
            List[str],
            Field(
                description="The names for dimensions of the data. Null or empty list "
                "if scalar data",
            ),
        ]
    ]
    dtype: Annotated[
        Dtype,
        Field(description="The type of the data in the event."),
    ]
    external: NotRequired[
        Annotated[
            str,
            Field(
                description="Where the data is stored if it is stored external "
                "to the events",
                pattern=r"^[A-Z]+:?",
            ),
        ]
    ]
    object_name: NotRequired[
        Annotated[
            str,
            Field(description="The name of the object this key was pulled from."),
        ]
    ]
    precision: NotRequired[
        Annotated[
            Optional[int],
            Field(
                description="Number of digits after decimal place if "
                "a floating point number"
            ),
        ]
    ]
    shape: Annotated[
        List[int],
        Field(description="The shape of the data.  Empty list indicates scalar data."),
    ]
    source: Annotated[
        str, Field(description="The source (ex piece of hardware) of the data.")
    ]
    units: NotRequired[
        Annotated[Optional[str], Field(description="Engineering units of the value")]
    ]


class PerObjectHint(TypedDict):
    """The 'interesting' data keys for this device."""

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
    data_keys: NotRequired[
        Annotated[
            Dict[str, DataKey],
            Field(
                description="This describes the data stored alongside it in this "
                "configuration object."
            ),
        ]
    ]
    timestamps: NotRequired[
        Annotated[
            Dict[str, Any],
            Field(description="The timestamps of the individual measurement data"),
        ]
    ]


EVENT_DESCRIPTOR_EXTRA_SCHEMA = {
    "patternProperties": {"^([^./]+)$": {"$ref": "#/$defs/DataType"}},
    "$defs": {
        "DataType": {
            "title": "DataType",
            "patternProperties": {"^([^./]+)$": {"$ref": "#/$defs/DataType"}},
            "additionalProperties": False,
        },
    },
    "additionalProperties": False,
}


@add_extra_schema(EVENT_DESCRIPTOR_EXTRA_SCHEMA)
class EventDescriptor(TypedDict):
    """Document to describe the data captured in the associated event
    documents"""

    configuration: NotRequired[
        Annotated[
            Dict[str, Configuration],
            Field(
                description="Readings of configurational fields necessary for "
                "interpreting data in the Events.",
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
    hints: NotRequired[PerObjectHint]
    name: NotRequired[
        Annotated[
            str,
            Field(
                description="A human-friendly name for this data stream, such as "
                "'primary' or 'baseline'.",
            ),
        ]
    ]
    object_keys: NotRequired[
        Annotated[
            Dict[str, Any],
            Field(
                description="Maps a Device/Signal name to the names of the entries "
                "it produces in data_keys.",
            ),
        ]
    ]
    run_start: Annotated[
        str, Field(description="Globally unique ID of this run's 'start' document.")
    ]
    time: Annotated[
        float, Field(description="Creation time of the document as unix epoch time.")
    ]
    uid: Annotated[
        str,
        Field(description="Globally unique ID for this event descriptor.", title="uid"),
    ]
