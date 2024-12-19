from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
)
from pydantic.config import JsonDict
from typing_extensions import Annotated, Literal

Dtype = Literal["string", "number", "array", "boolean", "integer"]


class DataType(RootModel):
    root: Any = Field(alias="DataType")


class LimitsRange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    low: Optional[float]
    high: Optional[float]


class Limits(BaseModel):
    """
    Epics limits:
    see 3.4.1 https://epics.anl.gov/base/R3-14/12-docs/AppDevGuide/node4.html
    """

    model_config = ConfigDict(extra="forbid")

    control: Annotated[
        Union[LimitsRange, None], Field(default=None, description="Control limits.")
    ]
    display: Annotated[
        Union[LimitsRange, None], Field(default=None, description="Display limits.")
    ]
    warning: Annotated[
        Union[LimitsRange, None], Field(default=None, description="Warning limits.")
    ]
    alarm: Annotated[
        Union[LimitsRange, None], Field(default=None, description="Alarm limits.")
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

    model_config = ConfigDict(extra="forbid")

    limits: Annotated[
        Union[Limits, None], Field(default=None, description="Epics limits.")
    ]
    choices: Annotated[
        Union[List[str], None],
        Field(default=None, description="Choices of enum value."),
    ]
    dims: Annotated[
        Union[List[str], None],
        Field(
            default=None,
            description="The names for dimensions of the data. Null or empty list "
            "if scalar data",
        ),
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
    dtype_numpy: Annotated[
        Union[_ConstrainedDtype, _ConstrainedDtypeNpStructure, None],
        Field(
            default=None,
            description=(
                "The type of the data in the event, given as a "
                "numpy dtype string (or, for structured dtypes, array)."
            ),
        ),
    ]
    external: Annotated[
        Union[str, None],
        Field(
            default=None,
            description="Where the data is stored if it is stored external "
            "to the events",
            pattern=r"^[A-Z]+:?",
        ),
    ]
    object_name: Annotated[
        Union[str, None],
        Field(
            default=None, description="The name of the object this key was pulled from."
        ),
    ]
    precision: Annotated[
        Union[int, None],
        Field(
            default=None,
            description="Number of digits after decimal place if "
            "a floating point number",
        ),
    ]
    shape: Annotated[
        List[int],
        Field(description="The shape of the data.  Empty list indicates scalar data."),
    ]
    source: Annotated[
        str, Field(description="The source (ex piece of hardware) of the data.")
    ]
    units: Annotated[
        Union[str, None],
        Field(default=None, description="Engineering units of the value"),
    ]


class PerObjectHint(BaseModel):
    """The 'interesting' data keys for this device."""

    model_config = ConfigDict(extra="forbid")

    fields: Annotated[
        Union[List[str], None],
        Field(default=None, description="The 'interesting' data keys for this device."),
    ]
    NX_class: Annotated[
        Union[str, None],
        Field(
            default=None,
            description="The NeXus class definition for this device.",
            pattern=r"^NX[A-Za-z_]+$",
        ),
    ]


class Configuration(BaseModel):
    model_config = ConfigDict(extra="forbid")
    data: Annotated[
        Union[Dict[str, Any], None],
        Field(default=None, description="The actual measurement data"),
    ]
    data_keys: Annotated[
        Union[Dict[str, DataKey], None],
        Field(
            default=None,
            description="This describes the data stored alongside it in this "
            "configuration object.",
        ),
    ]
    timestamps: Annotated[
        Union[Dict[str, Any], None],
        Field(
            default=None,
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
        extra="allow", json_schema_extra=EVENT_DESCRIPTOR_EXTRA_SCHEMA
    )

    configuration: Annotated[
        Union[Dict[str, Configuration], None],
        Field(
            default=None,
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
    hints: Annotated[Union[PerObjectHint, None], Field(default=None)]
    name: Annotated[
        str,
        Field(
            default="",
            description="A human-friendly name for this data stream, such as "
            "'primary' or 'baseline'.",
        ),
    ]
    object_keys: Annotated[
        Union[Dict[str, Any], None],
        Field(
            default=None,
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
