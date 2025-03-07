# ruff: noqa
# generated by datamodel-codegen:
#   filename:  run_start.json

from __future__ import annotations

from typing import Any, Dict, List, TypedDict, Union

from typing_extensions import NotRequired


class Calculation(TypedDict):
    args: NotRequired[List]
    callable: str
    """
    callable function to perform calculation
    """
    kwargs: NotRequired[Dict[str, Any]]
    """
    kwargs for calcalation callable
    """


class ConfigurationProjection(TypedDict):
    config_device: str
    config_index: int
    field: str
    location: NotRequired[str]
    """
    Projection comes from configuration fields in the event_descriptor document
    """
    stream: str
    type: NotRequired[str]
    """
    Projection is of type linked, a value linked from the data set.
    """


DataType = Any


class Hints(TypedDict):
    """
    Start-level hints
    """

    dimensions: NotRequired[List[List[Union[List[str], str]]]]
    """
    The independent axes of the experiment. Ordered slow to fast
    """


class LinkedEventProjection(TypedDict):
    field: str
    location: NotRequired[str]
    """
    Projection comes and event
    """
    stream: str
    type: NotRequired[str]
    """
    Projection is of type linked, a value linked from the data set.
    """


class StaticProjection(TypedDict):
    type: NotRequired[str]
    """
    Projection is of type static, a value defined here in the projection
    """
    value: Any
    """
    value explicitely defined in the static projection
    """


class CalculatedEventProjection(TypedDict):
    calculation: Calculation
    """
    required fields if type is calculated
    """
    field: str
    location: NotRequired[str]
    """
    Projection comes and event
    """
    stream: str
    type: NotRequired[str]
    """
    Projection is of type calculated, a value that requires calculation.
    """


class Projections(TypedDict):
    """
    Describe how to interperet this run as the given projection
    """

    configuration: Dict[str, Any]
    """
    Static information about projection
    """
    name: NotRequired[str]
    """
    The name of the projection
    """
    projection: Dict[
        str,
        Union[
            ConfigurationProjection,
            LinkedEventProjection,
            CalculatedEventProjection,
            StaticProjection,
        ],
    ]
    version: str
    """
    The version of the projection spec. Can specify the version of an external specification.
    """


class RunStart(TypedDict, total=False):
    """
    Document created at the start of run. Provides a seach target and
    later documents link to it
    """

    data_groups: NotRequired[List[str]]
    """
    An optional list of data access groups that have meaning to some external system. Examples might include facility, beamline, end stations, proposal, safety form.
    """
    data_session: NotRequired[str]
    """
    An optional field for grouping runs. The meaning is not mandated, but this is a data management grouping and not a scientific grouping. It is intended to group runs in a visit or set of trials.
    """
    data_type: NotRequired[DataType]
    group: NotRequired[str]
    """
    Unix group to associate this data with
    """
    hints: NotRequired[Hints]
    owner: NotRequired[str]
    """
    Unix owner to associate this data with
    """
    project: NotRequired[str]
    """
    Name of project that this run is part of
    """
    projections: NotRequired[List[Projections]]
    sample: NotRequired[Union[Dict[str, Any], str]]
    """
    Information about the sample, may be a UID to another collection
    """
    scan_id: NotRequired[int]
    """
    Scan ID number, not globally unique
    """
    time: float
    """
    Time the run started.  Unix epoch time
    """
    uid: str
    """
    Globally unique ID for this run
    """
