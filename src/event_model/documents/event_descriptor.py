from typing import Any, Dict, List, Optional, Tuple, Union

from typing_extensions import Annotated, Literal, NotRequired, TypedDict

from .generate.type_wrapper import Field, add_extra_schema

Dtype = Literal["string", "number", "array", "boolean", "integer"]


class RdsRange(TypedDict):
    """RDS (Read different than set) parameters range.


    https://tango-controls.readthedocs.io/en/latest/development/device-api/attribute-alarms.html#the-read-different-than-set-rds-alarm
    """

    time_difference: Annotated[
        float,
        Field(
            description=(
                "ms since last update to fail after if set point and "
                "read point are not within `value_difference` of each other."
            )
        ),
    ]
    value_difference: NotRequired[
        Annotated[
            float,
            Field(
                description=(
                    "Allowed difference in value between set point and read point "
                    "after `time_difference`."
                )
            ),
        ]
    ]


class LimitsRange(TypedDict):
    low: Optional[float]
    high: Optional[float]


class Limits(TypedDict):
    """
    Epics and tango limits:
    see 3.4.1 https://epics.anl.gov/base/R3-14/12-docs/AppDevGuide/node4.html
    and
    https://tango-controls.readthedocs.io/en/latest/development/device-api/attribute-alarms.html
    """

    control: NotRequired[Annotated[LimitsRange, Field(description="Control limits.")]]
    display: NotRequired[Annotated[LimitsRange, Field(description="Display limits.")]]
    warning: NotRequired[Annotated[LimitsRange, Field(description="Warning limits.")]]
    alarm: NotRequired[Annotated[LimitsRange, Field(description="Alarm limits.")]]
    hysteresis: NotRequired[Annotated[float, Field(description="Hysteresis.")]]
    rds: NotRequired[Annotated[RdsRange, Field(description="RDS parameters.")]]


_ConstrainedDtype = Annotated[
    str,
    Field(
        description="A numpy dtype e.g `<U9`, `<f16`",
        pattern="[|<>][tbiufcmMOSUV][0-9]+",
    ),
]

_ConstrainedDtypeNpStructure = List[Tuple[str, _ConstrainedDtype]]


class DataKey(TypedDict):
    """Describes the objects in the data property of Event documents"""

    limits: NotRequired[Annotated[Limits, Field(description="Epics limits.")]]
    choices: NotRequired[
        Annotated[List[str], Field(description="Choices of enum value.")]
    ]
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
        Field(
            description=(
                "The type of the data in the event, given as a broad "
                "JSON schema type."
            )
        ),
    ]
    dtype_numpy: NotRequired[
        Annotated[
            Union[_ConstrainedDtype, _ConstrainedDtypeNpStructure],
            Field(
                description=(
                    "The type of the data in the event, given as a "
                    "numpy dtype string (or, for structured dtypes, array)."
                )
            ),
        ]
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
    NX_class: NotRequired[
        Annotated[
            str,
            Field(
                description="The NeXus class definition for this device.",
                pattern=r"^NX[A-Za-z_]+$",
            ),
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
