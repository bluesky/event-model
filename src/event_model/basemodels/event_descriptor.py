# ruff: noqa
# type: ignore
# generated by datamodel-codegen:
#   filename:  event_descriptor.json

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, RootModel


class Dtype(Enum):
    """
    The type of the data in the event, given as a broad JSON schema type.
    """

    string = "string"
    number = "number"
    array = "array"
    boolean = "boolean"
    integer = "integer"


class DtypeNumpy(RootModel[str]):
    root: str = Field(..., pattern="[|<>][tbiufcmMOSUV][0-9]+", title="Dtype Numpy")
    """
    A numpy dtype e.g `<U9`, `<f16`
    """


class DtypeNumpyItem(RootModel[List]):
    root: List = Field(..., max_length=2, min_length=2)


class LimitsRange(BaseModel):
    high: Optional[float] = Field(..., title="High")
    low: Optional[float] = Field(..., title="Low")


class PerObjectHint(BaseModel):
    """
    The 'interesting' data keys for this device.
    """

    NX_class: Optional[str] = Field(None, pattern="^NX[A-Za-z_]+$", title="Nx Class")
    """
    The NeXus class definition for this device.
    """
    fields: Optional[List[str]] = Field(None, title="Fields")
    """
    The 'interesting' data keys for this device.
    """


class RdsRange(BaseModel):
    """
    RDS (Read different than set) parameters range.


    https://tango-controls.readthedocs.io/en/latest/development/device-api/attribute-alarms.html#the-read-different-than-set-rds-alarm
    """

    time_difference: float = Field(..., title="Time Difference")
    """
    ms since last update to fail after if set point and read point are not within `value_difference` of each other.
    """
    value_difference: Optional[float] = Field(None, title="Value Difference")
    """
    Allowed difference in value between set point and read point after `time_difference`.
    """


import re

from pydantic import field_validator


class DataType(RootModel):
    root: Dict[str, Union["DataType", Any]] = Field(..., title="DataType")

    @field_validator("root")
    def validate_root(cls, value):
        if not isinstance(value, dict):
            value
        pattern = r"^([^./]+)$"
        for key, val in value.items():
            if not re.match(pattern, key):
                raise ValueError(f"Key '{key}' does not match pattern '{pattern}'")
            if isinstance(val, dict):
                value[key] = DataType(**val)
        return value


class Limits(BaseModel):
    """
    Epics and tango limits:
    see 3.4.1 https://epics.anl.gov/base/R3-14/12-docs/AppDevGuide/node4.html
    and
    https://tango-controls.readthedocs.io/en/latest/development/device-api/attribute-alarms.html
    """

    alarm: Optional[LimitsRange] = None
    """
    Alarm limits.
    """
    control: Optional[LimitsRange] = None
    """
    Control limits.
    """
    display: Optional[LimitsRange] = None
    """
    Display limits.
    """
    hysteresis: Optional[float] = Field(None, title="Hysteresis")
    """
    Hysteresis.
    """
    rds: Optional[RdsRange] = None
    """
    RDS parameters.
    """
    warning: Optional[LimitsRange] = None
    """
    Warning limits.
    """


class DataKey(BaseModel):
    """
    Describes the objects in the data property of Event documents
    """

    choices: Optional[List[str]] = Field(None, title="Choices")
    """
    Choices of enum value.
    """
    dims: Optional[List[str]] = Field(None, title="Dims")
    """
    The names for dimensions of the data. Null or empty list if scalar data
    """
    dtype: Dtype = Field(..., title="Dtype")
    """
    The type of the data in the event, given as a broad JSON schema type.
    """
    dtype_numpy: Optional[Union[DtypeNumpy, List[DtypeNumpyItem]]] = Field(
        None, title="Dtype Numpy"
    )
    """
    The type of the data in the event, given as a numpy dtype string (or, for structured dtypes, array).
    """
    external: Optional[str] = Field(None, pattern="^[A-Z]+:?", title="External")
    """
    Where the data is stored if it is stored external to the events
    """
    limits: Optional[Limits] = None
    """
    Epics limits.
    """
    object_name: Optional[str] = Field(None, title="Object Name")
    """
    The name of the object this key was pulled from.
    """
    precision: Optional[int] = Field(None, title="Precision")
    """
    Number of digits after decimal place if a floating point number
    """
    shape: List[int] = Field(..., title="Shape")
    """
    The shape of the data.  Empty list indicates scalar data.
    """
    source: str = Field(..., title="Source")
    """
    The source (ex piece of hardware) of the data.
    """
    units: Optional[str] = Field(None, title="Units")
    """
    Engineering units of the value
    """


class Configuration(BaseModel):
    data: Optional[Dict[str, Any]] = Field(None, title="Data")
    """
    The actual measurement data
    """
    data_keys: Optional[Dict[str, DataKey]] = Field(None, title="Data Keys")
    """
    This describes the data stored alongside it in this configuration object.
    """
    timestamps: Optional[Dict[str, Any]] = Field(None, title="Timestamps")
    """
    The timestamps of the individual measurement data
    """


from pydantic import model_validator


class EventDescriptor(BaseModel):
    """
    Document to describe the data captured in the associated event documents
    """

    model_config = ConfigDict(
        extra="allow",
    )

    @model_validator(mode="before")
    def store_extra_values_as_datatype(cls, values):
        extra_values = {k: v for k, v in values.items() if k not in cls.model_fields}
        pattern = r"^([^./]+)$"
        for key, value in extra_values.items():
            if not re.match(pattern, key):
                raise ValueError(f"Key '{key}' does not match pattern '{pattern}'")
            values[key] = DataType(value)
        return values

    configuration: Optional[Dict[str, Configuration]] = Field(
        None, title="Configuration"
    )
    """
    Readings of configurational fields necessary for interpreting data in the Events.
    """
    data_keys: Dict[str, DataKey] = Field(..., title="data_keys")
    """
    This describes the data in the Event Documents.
    """
    hints: Optional[PerObjectHint] = None
    name: Optional[str] = Field(None, title="Name")
    """
    A human-friendly name for this data stream, such as 'primary' or 'baseline'.
    """
    object_keys: Optional[Dict[str, Any]] = Field(None, title="Object Keys")
    """
    Maps a Device/Signal name to the names of the entries it produces in data_keys.
    """
    run_start: str = Field(..., title="Run Start")
    """
    Globally unique ID of this run's 'start' document.
    """
    time: float = Field(..., title="Time")
    """
    Creation time of the document as unix epoch time.
    """
    uid: str = Field(..., title="uid")
    """
    Globally unique ID for this event descriptor.
    """


DataType.model_rebuild()
