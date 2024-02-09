from typing import Any, Dict, List, Union

from typing_extensions import Annotated, Literal, NotRequired, TypedDict

from event_model.documents.generate.type_wrapper import Field, add_extra_schema

from .generate.type_wrapper import DataType


class Hints(TypedDict):
    """Start-level hints"""

    dimensions: NotRequired[
        Annotated[
            List[List[Union[List[str], str]]],
            Field(
                description="The independent axes of the experiment. "
                "Ordered slow to fast",
            ),
        ]
    ]


class Calculation(TypedDict):
    args: NotRequired[List]
    callable: Annotated[
        str, Field(description="callable function to perform calculation")
    ]
    kwargs: NotRequired[
        Annotated[Dict[str, Any], Field(description="kwargs for calcalation callable")]
    ]


class Projection(TypedDict):
    """Where to get the data from"""

    calculation: NotRequired[
        Annotated[
            Calculation,
            Field(
                description="required fields if type is calculated",
                title="calculation properties",
            ),
        ]
    ]
    config_index: NotRequired[int]
    config_device: NotRequired[str]
    field: NotRequired[str]
    location: NotRequired[
        Annotated[
            Literal["start", "event", "configuration"],
            Field(
                description="start comes from metadata fields in the start document, "
                "event comes from event, configuration comes from configuration "
                "fields in the event_descriptor document"
            ),
        ]
    ]
    stream: NotRequired[str]
    type: NotRequired[
        Annotated[
            Literal["linked", "calculated", "static"],
            Field(
                description="linked: a value linked from the data set, "
                "calculated: a value that requires calculation, "
                "static:  a value defined here in the projection ",
            ),
        ]
    ]
    value: NotRequired[
        Annotated[
            Any,
            Field(
                description="value explicitely defined in the projection "
                "when type==static."
            ),
        ]
    ]


RUN_START_EXTRA_SCHEMA = {
    "$defs": {
        "DataType": {
            "patternProperties": {"^([^./]+)$": {"$ref": "#/$defs/DataType"}},
            "additionalProperties": False,
        },
        "Projection": {
            "allOf": [
                {
                    "if": {
                        "allOf": [
                            {"properties": {"location": {"enum": ["configuration"]}}},
                            {"properties": {"type": {"enum": ["linked"]}}},
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
                            {"properties": {"location": {"enum": ["event"]}}},
                            {"properties": {"type": {"enum": ["linked"]}}},
                        ]
                    },
                    "then": {"required": ["type", "location", "field", "stream"]},
                },
                {
                    "if": {
                        "allOf": [
                            {"properties": {"location": {"enum": ["event"]}}},
                            {"properties": {"type": {"enum": ["calculated"]}}},
                        ]
                    },
                    "then": {"required": ["type", "field", "stream", "calculation"]},
                },
                {
                    "if": {"properties": {"type": {"enum": ["static"]}}},
                    "then": {"required": ["type", "value"]},
                },
            ],
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


class Projections(TypedDict):
    """Describe how to interperet this run as the given projection"""

    configuration: Annotated[
        Dict[str, Any], Field(description="Static information about projection")
    ]
    name: NotRequired[Annotated[str, Field(description="The name of the projection")]]
    projection: Annotated[Dict[Any, Projection], Field(description="")]
    version: Annotated[
        str,
        Field(
            description="The version of the projection spec. Can specify the version "
            "of an external specification.",
        ),
    ]


@add_extra_schema(RUN_START_EXTRA_SCHEMA)
class RunStart(TypedDict):
    """
    Document created at the start of run. Provides a seach target and
    later documents link to it
    """

    data_groups: NotRequired[
        Annotated[
            List[str],
            Field(
                description="An optional list of data access groups that have meaning "
                "to some external system. Examples might include facility, beamline, "
                "end stations, proposal, safety form.",
            ),
        ]
    ]
    data_session: NotRequired[
        Annotated[
            str,
            Field(
                description="An optional field for grouping runs. The meaning is "
                "not mandated, but this is a data management grouping and not a "
                "scientific grouping. It is intended to group runs in a visit or "
                "set of trials.",
            ),
        ]
    ]
    data_type: NotRequired[Annotated[DataType, Field(description="")]]
    group: NotRequired[
        Annotated[str, Field(description="Unix group to associate this data with")]
    ]
    hints: NotRequired[Annotated[Hints, Field(description="Start-level hints")]]
    owner: NotRequired[
        Annotated[str, Field(description="Unix owner to associate this data with")]
    ]
    project: NotRequired[
        Annotated[str, Field(description="Name of project that this run is part of")]
    ]
    projections: NotRequired[Annotated[List[Projections], Field(description="")]]
    sample: NotRequired[
        Annotated[
            Union[Dict[str, Any], str],
            Field(
                description="Information about the sample, may be a UID to "
                "another collection"
            ),
        ]
    ]
    scan_id: NotRequired[
        Annotated[int, Field(description="Scan ID number, not globally unique")]
    ]
    time: Annotated[float, Field(description="Time the run started.  Unix epoch time")]
    uid: Annotated[str, Field(description="Globally unique ID for this run")]
