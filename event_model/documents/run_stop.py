from typing import Any, Dict, TYPE_CHECKING, TypedDict, Literal

from ._type_wrapper import Field, Annotated, Optional


if TYPE_CHECKING:
    DataType = Any
else:

    class DataType(TypedDict):
        __root__: Annotated[str, Field(title="data_type", regex="^([^./]+)$")]


class RunStopOptional(TypedDict, total=False):
    reason: Annotated[
        Optional[str], Field(description="Long-form description of why the run ended")
    ]

    num_events: Annotated[
        Optional[Dict[str, int]],
        Field(
            description="Number of Events per named stream",
        ),
    ]
    data_type: Annotated[Optional[DataType], Field(description="")]


class RunStop(RunStopOptional):
    """Document for the end of a run indicating the success/fail state of the run and the end time"""

    run_start: Annotated[
        str,
        Field(
            description="Reference back to the run_start document that this document is paired with.",
        ),
    ]
    time: Annotated[float, Field(description="The time the run ended. Unix epoch")]
    exit_status: Annotated[
        Literal["success", "abort", "fail"],
        Field(description="State of the run when it ended"),
    ]
    uid: Annotated[str, Field(description="Globally unique ID for this document")]
