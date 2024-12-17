from typing import Dict, Optional

from typing_extensions import Annotated, Literal

from event_model.generate.type_wrapper import (
    BaseModel,
    ConfigDict,
    DataType,
    Field,
    ValidationError,
    model_validator,
)

RUN_STOP_EXTRA_SCHEMA = {
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
        Optional[DataType], Field(description="data_type", default=None)
    ]
    exit_status: Annotated[
        Literal["success", "abort", "fail"],
        Field(description="State of the run when it ended"),
    ]
    num_events: Annotated[
        Optional[Dict[str, int]],
        Field(
            description="Number of Events per named stream",
            default=None,
        ),
    ]
    reason: Annotated[
        Optional[str],
        Field(description="Long-form description of why the run ended", default=None),
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
    def validate_additional_fields(cls, values):
        for key, value in values.items():
            if "." not in key and key not in cls.__fields__:
                try:
                    DataType(value)
                except ValidationError as err:
                    raise ValueError(f"Extra non-datatype {key} received.") from err
        return values
