from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated


class StreamResource(BaseModel):
    """
    Document to reference a collection (e.g. file or group of files) of
    externally-stored data streams
    """

    model_config = ConfigDict(
        extra="allow",
    )

    data_key: Annotated[
        str,
        Field(
            description="A string to show which data_key of the "
            "Descriptor are being streamed"
        ),
    ]
    parameters: Annotated[
        Dict[str, Any],
        Field(
            description="Additional keyword arguments to pass to the Handler to read a "
            "Stream Resource",
        ),
    ]
    uri: Annotated[str, Field(description="URI for locating this resource")]
    run_start: Annotated[
        str,
        Field(
            description="Globally unique ID to the run_start document "
            "this Stream Resource is associated with.",
            default="",
        ),
    ]
    mimetype: Annotated[
        str,
        Field(
            description="String identifying the format/type of this Stream Resource, "
            "used to identify a compatible Handler",
        ),
    ]
    uid: Annotated[
        str, Field(description="Globally unique identifier for this Stream Resource")
    ]
