from typing import Any, Dict

from typing_extensions import Annotated

from event_model.generate.type_wrapper import BaseModel, Field


class StreamResource(BaseModel):
    """
    Document to reference a collection (e.g. file or group of files) of
    externally-stored data streams
    """

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