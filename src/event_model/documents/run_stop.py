from typing import Dict

from typing_extensions import Annotated, Literal, NotRequired, TypedDict

from .generate.type_wrapper import DataType, Field, add_extra_schema

RUN_STOP_EXTRA_SCHEMA = {
    "patternProperties": {"^([^./]+)$": {"$ref": "#/$defs/DataType"}},
    "additionalProperties": False,
}


@add_extra_schema(RUN_STOP_EXTRA_SCHEMA)
class RunStop(TypedDict):
    """
    Document for the end of a run indicating the success/fail state of the
    run and the end time
    """

    data_type: NotRequired[Annotated[DataType, Field(description="data_type")]]
    exit_status: Annotated[
        Literal["success", "abort", "fail"],
        Field(description="State of the run when it ended"),
    ]
    num_events: NotRequired[
        Annotated[
            Dict[str, int],
            Field(
                description="Number of Events per named stream",
            ),
        ]
    ]
    reason: NotRequired[
        Annotated[str, Field(description="Long-form description of why the run ended")]
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
