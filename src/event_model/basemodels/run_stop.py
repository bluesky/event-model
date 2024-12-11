import re
from typing import Any, Dict

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


RUN_STOP_EXTRA_SCHEMA: JsonDict = {
    "patternProperties": {"^([^./]+)$": {"$ref": "#/$defs/DataType"}},
    "additionalProperties": False,
}


class RunStop(BaseModel):
    """
    Document for the end of a run indicating the success/fail state of the
    run and the end time
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra=RUN_STOP_EXTRA_SCHEMA,
    )

    data_type: Annotated[
        DataType,
        Field(description="data_type", default_factory=lambda: DataType(root=None)),
    ]
    exit_status: Annotated[
        Literal["success", "abort", "fail"],
        Field(description="State of the run when it ended"),
    ]
    num_events: Annotated[
        Dict[str, int],
        Field(
            description="Number of Events per named stream",
            default={},
        ),
    ]
    reason: Annotated[
        str,
        Field(description="Long-form description of why the run ended", default=""),
    ]
    run_start: Annotated[
        str,
        Field(
            description="Reference back to the run_start document that this document "
            "is paired with.",
        ),
    ]
    time: Annotated[float, Field(description="The time the run ended. Unix epoch")]
    uid: Annotated[str, Field(description="Globally unique ID for this document")]

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
