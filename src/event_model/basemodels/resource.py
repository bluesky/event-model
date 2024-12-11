from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated, Literal


class PartialResource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spec: Annotated[
        str,
        Field(
            description="String identifying the format/type of this Resource, used to "
            "identify a compatible Handler"
        ),
    ]
    resource_path: Annotated[
        str, Field(description="Filepath or URI for locating this resource")
    ]
    resource_kwargs: Annotated[
        Dict[str, Any],
        Field(
            description="Additional argument to pass to the Handler to read a Resource"
        ),
    ]

    root: Annotated[
        str,
        Field(
            description="Subset of resource_path that is a local detail, not semantic."
        ),
    ]
    uid: Annotated[
        str, Field(description="Globally unique identifier for this Resource")
    ]


class Resource(PartialResource):
    """
    Document to reference a collection (e.g. file or group of files) of
    externally-stored data
    """

    model_config = ConfigDict(extra="forbid")

    path_semantics: Annotated[
        Literal["posix", "windows"],
        Field(description="Rules for joining paths", default="posix"),
    ]
    run_start: Annotated[
        str,
        Field(
            description="Globally unique ID to the run_start document this "
            "resource is associated with.",
            default="",
        ),
    ]
