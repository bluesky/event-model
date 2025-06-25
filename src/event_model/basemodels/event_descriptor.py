import re
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_validator,
    model_validator,
)
from pydantic.config import JsonDict
from typing_extensions import Annotated, Literal


class Dtype(RootModel):
    root: Literal["string", "number", "array", "boolean", "integer"]


NO_DOTS_PATTERN = r"^([^./]+)$"


class DataType(RootModel):
    root: Any = Field(alias="DataType")

    @field_validator("root")
    def validate_root(cls, value):
        if not isinstance(value, dict):
            return value
        for key, val in value.items():
            if not re.match(NO_DOTS_PATTERN, key):
                raise ValueError(
                    f"Key '{key}' does not match pattern '{NO_DOTS_PATTERN}'"
                )
            if isinstance(val, dict):
                value[key] = DataType(val)
        return value


class LimitsRange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    low: Optional[float]
    high: Optional[float]


class RdsRange(BaseModel):
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
    value_difference: Annotated[
        float,
        Field(
            description=(
                "Allowed difference in value between set point and read point "
                "after `time_difference`."
            ),
        ),
    ]


class Limits(BaseModel):
    """
    Epics limits:
    https://docs.epics-controls.org/en/latest/getting-started/EPICS_Intro.html#channel-access
    """

    model_config = ConfigDict(extra="forbid")

    control: Annotated[
        Optional[LimitsRange], Field(default=None, description="Control limits.")
    ]
    display: Annotated[
        Optional[LimitsRange], Field(default=None, description="Display limits.")
    ]
    warning: Annotated[
        Optional[LimitsRange], Field(default=None, description="Warning limits.")
    ]
    alarm: Annotated[
        Optional[LimitsRange], Field(default=None, description="Alarm limits.")
    ]

    hysteresis: Annotated[
        Optional[float], Field(default=None, description="Hysteresis.")
    ]
    rds: Annotated[
        Optional[RdsRange], Field(default=None, description="RDS parameters.")
    ]


_ConstrainedDtype = Annotated[
    str,
    Field(
        description="A numpy dtype e.g `<U9`, `<f16`",
        pattern="[|<>][tbiufcmMOSUV][0-9]+",
    ),
]

_ConstrainedDtypeNpStructure = List[Tuple[str, _ConstrainedDtype]]


class DataKey(BaseModel):
    """Describes the objects in the data property of Event documents"""

    model_config = ConfigDict(extra="allow")

    limits: Annotated[
        Limits,
        Field(
            default_factory=lambda: Limits(
                control=None,
                display=None,
                warning=None,
                alarm=None,
                hysteresis=None,
                rds=None,
            ),
            description="Epics limits.",
        ),
    ]
    choices: Annotated[
        List[str],
        Field(default=[], description="Choices of enum value."),
    ]
    dims: Annotated[
        List[str],
        Field(
            default=[],
            description="The names for dimensions of the data. Null or empty list "
            "if scalar data",
        ),
    ]
    dtype: Annotated[
        Dtype,
        Field(
            description=(
                "The type of the data in the event, given as a broad JSON schema type."
            )
        ),
    ]
    dtype_numpy: Annotated[
        Union[_ConstrainedDtype, _ConstrainedDtypeNpStructure],
        Field(
            default="",
            validate_default=False,
            description=(
                "The type of the data in the event, given as a "
                "numpy dtype string (or, for structured dtypes, array)."
            ),
        ),
    ]
    external: Annotated[
        str,
        Field(
            default="",
            validate_default=False,
            description="Where the data is stored if it is stored external "
            "to the events",
            pattern=r"^[A-Z]+:?",
        ),
    ]
    object_name: Annotated[
        str,
        Field(
            default="", description="The name of the object this key was pulled from."
        ),
    ]
    precision: Annotated[
        Optional[int],
        Field(
            default=None,
            description="Number of digits after decimal place if "
            "a floating point number",
        ),
    ]
    shape: Annotated[
        List[Optional[int]],
        Field(
            description="The shape of the data.  Empty list indicates scalar data. "
            "None indicates a dimension with unknown or variable length."
        ),
    ]
    source: Annotated[
        str, Field(description="The source (ex piece of hardware) of the data.")
    ]
    units: Annotated[
        Optional[str],
        Field(default=None, description="Engineering units of the value"),
    ]


class PerObjectHint(BaseModel):
    """The 'interesting' data keys for this device."""

    model_config = ConfigDict(extra="allow")

    fields: Annotated[
        List[str],
        Field(description="The 'interesting' data keys for this device."),
    ] = []
    NX_class: Annotated[
        str,
        Field(
            validate_default=False,
            description="The NeXus class definition for this device.",
            pattern=r"^NX[A-Za-z_]+$",
        ),
    ] = ""


class Configuration(BaseModel):
    model_config = ConfigDict(extra="allow")
    data: Annotated[
        Dict[str, Any],
        Field(default={}, description="The actual measurement data"),
    ]
    data_keys: Annotated[
        Dict[str, DataKey],
        Field(
            default={},
            description="This describes the data stored alongside it in this "
            "configuration object.",
        ),
    ]
    timestamps: Annotated[
        Dict[str, Any],
        Field(
            default={},
            description="The timestamps of the individual measurement data",
        ),
    ]


EVENT_DESCRIPTOR_EXTRA_SCHEMA: JsonDict = {
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


class EventDescriptor(BaseModel):
    """Document to describe the data captured in the associated event
    documents"""

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra=EVENT_DESCRIPTOR_EXTRA_SCHEMA,
    )

    configuration: Annotated[
        Dict[str, Configuration],
        Field(
            default={},
            description="Readings of configurational fields necessary for "
            "interpreting data in the Events.",
        ),
    ]
    data_keys: Annotated[
        Dict[str, DataKey],
        Field(
            description="This describes the data in the Event Documents.",
            title="data_keys",
        ),
    ]
    hints: Annotated[PerObjectHint, Field(default_factory=lambda: PerObjectHint())]
    name: Annotated[
        str,
        Field(
            default="",
            description="A human-friendly name for this data stream, such as "
            "'primary' or 'baseline'.",
        ),
    ]
    object_keys: Annotated[
        Dict[str, Any],
        Field(
            default={},
            description="Maps a Device/Signal name to the names of the entries "
            "it produces in data_keys.",
        ),
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

    @model_validator(mode="before")
    @classmethod
    def store_extra_values_as_datatype(cls, values):
        extra_values = {k: v for k, v in values.items() if k not in cls.model_fields}
        for key, value in extra_values.items():
            if not re.match(NO_DOTS_PATTERN, key):
                raise ValueError(
                    f"Key '{key}' does not match pattern '{NO_DOTS_PATTERN}'"
                )
            if isinstance(value, dict):
                values[key] = DataType(value)
        return values
