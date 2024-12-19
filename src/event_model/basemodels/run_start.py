from typing import Any, Dict, List, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
)
from pydantic.config import JsonDict
from typing_extensions import Annotated, Literal


class DataType(RootModel):
    root: Any = Field(alias="DataType")


class Hints(BaseModel):
    """Start-level hints"""

    model_config = ConfigDict(extra="forbid")

    dimensions: Annotated[
        Optional[List[List[Union[List[str], str]]]],
        Field(
            description="The independent axes of the experiment. "
            "Ordered slow to fast",
            default=None,
        ),
    ]


class Calculation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    args: Annotated[Optional[List], Field(default=None)]
    callable: Annotated[
        str, Field(description="callable function to perform calculation")
    ]
    kwargs: Annotated[
        Optional[Dict[str, Any]],
        Field(description="kwargs for calcalation callable", default=None),
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
    name: Annotated[
        Optional[str], Field(description="The name of the projection", default=None)
    ]
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
        Optional[List[str]],
        Field(
            description="An optional list of data access groups that have meaning "
            "to some external system. Examples might include facility, beamline, "
            "end stations, proposal, safety form.",
            default=None,
        ),
    ]
    data_session: Annotated[
        Optional[str],
        Field(
            description="An optional field for grouping runs. The meaning is "
            "not mandated, but this is a data management grouping and not a "
            "scientific grouping. It is intended to group runs in a visit or "
            "set of trials.",
            default=None,
        ),
    ]
    data_type: Annotated[Optional[DataType], Field(description="", default=None)]
    group: Annotated[
        Optional[str],
        Field(description="Unix group to associate this data with", default=None),
    ]
    hints: Annotated[
        Optional[Hints], Field(description="Start-level hints", default=None)
    ]
    owner: Annotated[
        Optional[str],
        Field(description="Unix owner to associate this data with", default=None),
    ]
    project: Annotated[
        Optional[str],
        Field(description="Name of project that this run is part of", default=None),
    ]
    projections: Annotated[
        Optional[List[Projections]], Field(description="", default=None)
    ]
    sample: Annotated[
        Optional[Union[Dict[str, Any], str]],
        Field(
            description="Information about the sample, may be a UID to "
            "another collection",
            default=None,
        ),
    ]
    scan_id: Annotated[
        Optional[int],
        Field(description="Scan ID number, not globally unique", default=None),
    ]
    time: Annotated[float, Field(description="Time the run started.  Unix epoch time")]
    uid: Annotated[str, Field(description="Globally unique ID for this run")]
