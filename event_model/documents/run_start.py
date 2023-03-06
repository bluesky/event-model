from typing import Any, Dict, List, Union, TYPE_CHECKING, TypedDict, Literal

from event_model.documents._type_wrapper import (
    Field,
    Annotated,
    Optional,
)
from event_model.documents._type_wrapper import add_extra_schema


class Hints(TypedDict):
    dimensions: Optional[
        Annotated[
            List[List[Union[List[str], str]]],
            Field(
                description="The independent axes of the experiment.  Ordered slow to fast",
            ),
        ]
    ]


if TYPE_CHECKING:
    DataType = Any
else:

    class DataType(TypedDict):
        __root__: Annotated[Any, Field(title="data_type")]


class Calculation(TypedDict):
    args: Optional[List]

    kwargs: Optional[
        Annotated[Dict[str, Any], Field(description="kwargs for calcalation callable")]
    ]

    callable: Annotated[
        str, Field(description="callable function to perform calculation")
    ]


class Projection(TypedDict):
    type: Optional[
        Annotated[
            Literal["linked", "calculated", "static"],
            Field(
                description="linked: a value linked from the data set, "
                "calculated: a value that requires calculation, "
                "static:  a value defined here in the projection ",
            ),
        ]
    ]
    stream: Optional[str]
    location: Optional[
        Annotated[
            Literal["start", "event", "configuration"],
            Field(
                description="start comes from metadata fields in the start document, event comes from event, "
                "configuration comes from configuration fields in the event_descriptor document"
            ),
        ]
    ]
    field: Optional[str]
    config_index: Optional[int]
    config_device: Optional[str]
    calculation: Optional[
        Annotated[
            Calculation,
            Field(
                description="required fields if type is calculated",
                title="calculation properties",
            ),
        ]
    ]
    value: Optional[
        Annotated[
            Any,
            Field(
                description="value explicitely defined in the projection when type==static."
            ),
        ]
    ]


RUN_START_EXTRA_SCHEMA = {
    "definitions": {
        "Projection": {
            "allOf": [
                {
                    "if": {
                        "allOf": [
                            {"properties": {"location": {"enum": "configuration"}}},
                            {"properties": {"type": {"enum": "linked"}}},
                        ]
                    },
                    "then": {
                        "required": [
                            "type",
                            "location",
                            "config_index",
                            "config_device",
                            "field",
                            "stream",
                        ]
                    },
                },
                {
                    "if": {
                        "allOf": [
                            {"properties": {"location": {"enum": "event"}}},
                            {"properties": {"type": {"enum": "linked"}}},
                        ]
                    },
                    "then": {"required": ["type", "location", "field", "stream"]},
                },
                {
                    "if": {
                        "allOf": [
                            {"properties": {"location": {"enum": "event"}}},
                            {"properties": {"type": {"enum": "calculated"}}},
                        ]
                    },
                    "then": {"required": ["type", "field", "stream", "calculation"]},
                },
                {
                    "if": {"properties": {"type": {"enum": "static"}}},
                    "then": {"required": ["type", "value"]},
                },
            ],
        }
    }
}


class Projections(TypedDict):
    """Describe how to interperet this run as the given projection"""

    name: Optional[Annotated[str, Field(description="The name of the projection")]]
    version: Annotated[
        str,
        Field(
            description="The version of the projection spec. Can specify the version of "
            "an external specification.",
        ),
    ]
    configuration: Annotated[
        Dict[str, Any], Field(description="Static information about projection")
    ]
    projection: Dict[str, Projection]


@add_extra_schema(RUN_START_EXTRA_SCHEMA)
class RunStart(TypedDict):
    """Document created at the start of run.  Provides a seach target and later documents link to it"""

    data_session: Optional[
        Annotated[
            str,
            Field(
                description="An optional field for grouping runs. The meaning is not mandated, but "
                "this is a data management grouping and not a scientific grouping. It is intended to group "
                "runs in a visit or set of trials.",
            ),
        ]
    ]
    data_groups: Optional[
        Annotated[
            List[str],
            Field(
                description="An optional list of data access groups that have meaning to some external system. "
                "Examples might include facility, beamline, end stations, proposal, safety form.",
            ),
        ]
    ]
    project: Optional[
        Annotated[str, Field(description="Name of project that this run is part of")]
    ]
    sample: Optional[
        Annotated[
            Union[Dict[str, Any], str],
            Field(
                description="Information about the sample, may be a UID to another collection"
            ),
        ]
    ]
    scan_id: Optional[
        Annotated[int, Field(description="Scan ID number, not globally unique")]
    ]
    group: Optional[
        Annotated[str, Field(description="Unix group to associate this data with")]
    ]
    owner: Optional[
        Annotated[str, Field(description="Unix owner to associate this data with")]
    ]
    projections: Optional[Annotated[List[Projections], Field(description="")]]
    hints: Optional[Annotated[Hints, Field(description="Start-level hints")]]
    data_type: Optional[Annotated[DataType, Field(description="")]]

    time: Annotated[float, Field(description="Time the run started.  Unix epoch time")]
    uid: Annotated[str, Field(description="Globally unique ID for this run")]
