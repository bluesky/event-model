from typing import Any, Dict, List, Optional, Union

from typing_extensions import Annotated, Literal

from event_model.generate.type_wrapper import (
    BaseModel,
    ConfigDict,
    DataType,
    Field,
    model_validator,
)


class Hints(BaseModel):
    """Start-level hints"""

    dimensions: Annotated[
        Optional[List[List[Union[List[str], str]]]],
        Field(
            description="The independent axes of the experiment. "
            "Ordered slow to fast",
            default=None,
        ),
    ]


class Calculation(BaseModel):
    args: Optional[List]
    callable: Annotated[
        str, Field(description="callable function to perform calculation")
    ]
    kwargs: Annotated[
        Optional[Dict[str, Any]],
        Field(description="kwargs for calcalation callable", default=None),
    ]


class Projection(BaseModel):
    """Where to get the data from"""

    calculation: Annotated[
        Optional[Calculation],
        Field(
            description="required fields if type is calculated",
            title="calculation properties",
            default=None,
        ),
    ]
    config_index: Annotated[Optional[int], Field(default=None)]
    config_device: Annotated[Optional[str], Field(default=None)]
    field: Annotated[Optional[str], Field(default=None)]
    location: Annotated[
        Optional[Literal["start", "event", "configuration"]],
        Field(
            description="start comes from metadata fields in the start document, "
            "event comes from event, configuration comes from configuration "
            "fields in the event_descriptor document",
            default=None,
        ),
    ]
    stream: Annotated[Optional[str], Field(default=None)]
    type: Annotated[
        Optional[Literal["linked", "calculated", "static"]],
        Field(
            description="linked: a value linked from the data set, "
            "calculated: a value that requires calculation, "
            "static:  a value defined here in the projection ",
            default=None,
        ),
    ]
    value: Annotated[
        Optional[Any],
        Field(
            description="value explicitely defined in the projection "
            "when type==static.",
            default=None,
        ),
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


class Projections(BaseModel):
    """Describe how to interperet this run as the given projection"""

    configuration: Annotated[
        Dict[str, Any], Field(description="Static information about projection")
    ]
    name: Annotated[
        Optional[str], Field(description="The name of the projection", default=None)
    ]
    projection: Annotated[Dict[Any, Projection], Field(description="")]
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

    __config__ = ConfigDict(extra="allow", json_schema_extra=RUN_START_EXTRA_SCHEMA)

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

    @model_validator(mode="before")
    def check_required_fields(cls, values):
        type_ = values.get("type")
        location = values.get("location")

        if type_ == "linked" and location == "configuration":
            required_fields = [
                "type",
                "location",
                "config_index",
                "config_device",
                "field",
                "stream",
            ]
        elif type_ == "linked" and location == "event":
            required_fields = ["type", "location", "field", "stream"]
        elif type_ == "calculated" and location == "event":
            required_fields = ["type", "field", "stream", "calculation"]
        elif type_ == "static":
            required_fields = ["type", "value"]
        else:
            required_fields = []

        for field in required_fields:
            if values.get(field) is None:
                raise ValueError(
                    f"{field} is required for type {type_} and location {location}"
                )

        return values
