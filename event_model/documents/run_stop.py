from typing import Dict, Any

from typing_extensions import Annotated, NotRequired, Literal, TypedDict

from .generate.type_wrapper import Field, AsRef


class RunStop(TypedDict):
    """Document for the end of a run indicating the success/fail state of the run and the end time"""

    reason: NotRequired[
        Annotated[str, Field(description="Long-form description of why the run ended")]
    ]

    num_events: NotRequired[
        Annotated[
            Dict[str, int],
            Field(
                description="Number of Events per named stream",
            ),
        ]
    ]
    data_type: NotRequired[
        Annotated[
            Any, Field(description="data_type", regex="^([^./]+)$"), AsRef("DataType")
        ]
    ]

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
