import re
from typing import Any, Dict, List, Union

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

NO_DOTS_PATTERN = r"^([^./]+)$"


class DataType(RootModel):
    root: Any = Field(default=None)

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


class Hints(BaseModel):
    """Start-level hints"""

    model_config = ConfigDict(extra="allow")

    dimensions: Annotated[
        List[List[Union[List[str], str]]],
        Field(
            description="The independent axes of the experiment. Ordered slow to fast",
            default=[],
            validate_default=False,
        ),
    ]


class Calculation(BaseModel):
    model_config = ConfigDict(extra="allow")

    args: Annotated[List, Field(default=[])]
    callable: Annotated[
        str, Field(description="callable function to perform calculation")
    ]
    kwargs: Annotated[
        Dict[str, Any],
        Field(description="kwargs for calcalation callable", default={}),
    ]


class ConfigurationProjection(BaseModel):
    location: Annotated[
        Literal["configuration"],
        Field(
            description="Projection comes from configuration "
            "fields in the event_descriptor document",
        ),
    ]

    type: Annotated[
        Literal["linked"],
        Field(
            description=(
                "Projection is of type linked, a value linked from the data set."
            )
        ),
    ]
    config_index: Annotated[int, Field()]
    config_device: Annotated[str, Field()]
    field: Annotated[str, Field()]
    stream: Annotated[str, Field()]


class LinkedEventProjection(BaseModel):
    location: Annotated[
        Literal["event"],
        Field(description="Projection comes and event"),
    ]

    type: Annotated[
        Literal["linked"],
        Field(
            description=(
                "Projection is of type linked, a value linked from the data set."
            )
        ),
    ]
    field: Annotated[str, Field()]
    stream: Annotated[str, Field()]


class CalculatedEventProjection(BaseModel):
    location: Annotated[
        Literal["event"],
        Field(description="Projection comes and event"),
    ]

    type: Annotated[
        Literal["calculated"],
        Field(
            description=(
                "Projection is of type calculated, a value that requires calculation."
            )
        ),
    ]
    field: Annotated[str, Field()]
    stream: Annotated[str, Field()]
    calculation: Annotated[
        Calculation,
        Field(
            description="required fields if type is calculated",
            title="calculation properties",
        ),
    ]


class StaticProjection(BaseModel):
    type: Annotated[
        Literal["static"],
        Field(
            description=(
                "Projection is of type static, a value defined here in the projection"
            )
        ),
    ]
    value: Annotated[
        Any, Field(description="value explicitely defined in the static projection")
    ]


RUN_START_EXTRA_SCHEMA: JsonDict = {
    "$defs": {
        "DataType": {
            "patternProperties": {"^([^./]+)$": {"$ref": "#/$defs/DataType"}},
            "additionalProperties": False,
        },
    },
    "properties": {
        "hints": {
            "additionalProperties": False,
            "patternProperties": {"^([^.]+)$": {"$ref": "#/$defs/DataType"}},
        },
    },
    "patternProperties": {"^([^./]+)$": {"$ref": "#/$defs/DataType"}},
    "additionalProperties": False,
}


class Projections(BaseModel):
    """Describe how to interperet this run as the given projection"""

    configuration: Annotated[
        Dict[str, Any], Field(description="Static information about projection")
    ]
    name: Annotated[str, Field(description="The name of the projection", default="")]
    projection: Annotated[
        Dict[
            Any,
            Union[
                ConfigurationProjection,
                LinkedEventProjection,
                CalculatedEventProjection,
                StaticProjection,
            ],
        ],
        Field(description=""),
    ]
    version: Annotated[
        str,
        Field(
            description="The version of the projection spec. Can specify the version "
            "of an external specification.",
        ),
    ]


class RunStart(BaseModel):
    """
    Document created at the start of run. Provides a seach target and
    later documents link to it
    """

    model_config = ConfigDict(extra="allow", json_schema_extra=RUN_START_EXTRA_SCHEMA)

    data_groups: Annotated[
        List[str],
        Field(
            description="An optional list of data access groups that have meaning "
            "to some external system. Examples might include facility, beamline, "
            "end stations, proposal, safety form.",
            default=[],
        ),
    ]
    data_session: Annotated[
        str,
        Field(
            description="An optional field for grouping runs. The meaning is "
            "not mandated, but this is a data management grouping and not a "
            "scientific grouping. It is intended to group runs in a visit or "
            "set of trials.",
            default="",
        ),
    ]
    data_type: Annotated[DataType, Field(description="", default=DataType())]
    group: Annotated[
        str,
        Field(description="Unix group to associate this data with", default=""),
    ]
    hints: Annotated[
        Hints,
        Field(
            description="Start-level hints",
            default_factory=lambda: Hints(dimensions=[]),
        ),
    ]
    owner: Annotated[
        str,
        Field(description="Unix owner to associate this data with", default=""),
    ]
    project: Annotated[
        str,
        Field(description="Name of project that this run is part of", default=""),
    ]
    projections: Annotated[List[Projections], Field(description="", default=[])]
    sample: Annotated[
        Union[Dict[str, Any], str],
        Field(
            description="Information about the sample, may be a UID to "
            "another collection",
            default="",
        ),
    ]
    scan_id: Annotated[
        int,
        Field(description="Scan ID number, not globally unique", default=0),
    ]
    time: Annotated[float, Field(description="Time the run started.  Unix epoch time")]
    uid: Annotated[str, Field(description="Globally unique ID for this run")]

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
                values[key] = DataType(root=value)
        return values
