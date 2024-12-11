# ruff: noqa
# generated by datamodel-codegen:
#   filename:  run_stop.json

from __future__ import annotations

from typing import Any, Dict, Literal, TypedDict

from typing_extensions import NotRequired

DataType = Any


class RunStop(TypedDict):
    """
    Document for the end of a run indicating the success/fail state of the
    run and the end time
    """

    data_type: NotRequired[DataType]
    """
    data_type
    """
    exit_status: Literal["success", "abort", "fail"]
    """
    State of the run when it ended
    """
    num_events: NotRequired[Dict[str, int]]
    """
    Number of Events per named stream
    """
    reason: NotRequired[str]
    """
    Long-form description of why the run ended
    """
    run_start: str
    """
    Reference back to the run_start document that this document is paired with.
    """
    time: float
    """
    The time the run ended. Unix epoch
    """
    uid: str
    """
    Globally unique ID for this document
    """
