from typing import Any, Dict, List

from ._type_wrapper import Field, Annotated, Optional, TypedDict, Literal


class StreamResourceOptional(TypedDict, total=False):
    path_semantics: Annotated[
        Optional[Literal["posix", "windows"]],
        Field(description="Rules for joining paths"),
    ]
    run_start: Annotated[
        Optional[str],
        Field(
            description="Globally unique ID to the run_start document this Stream Resource is associated with.",
        ),
    ]


class StreamResource(StreamResourceOptional):
    """Document to reference a collection (e.g. file or group of files) of externally-stored data streams"""

    spec: Annotated[
        str,
        Field(
            description="String identifying the format/type of this Stream Resource, "
            "used to identify a compatible Handler",
        ),
    ]
    resource_path: Annotated[
        str, Field(description="Filepath or URI for locating this resource")
    ]
    resource_kwargs: Annotated[
        Dict[str, Any],
        Field(
            description="Additional argument to pass to the Handler to read a Stream Resource",
        ),
    ]
    root: Annotated[
        str,
        Field(
            description="Subset of resource_path that is a local detail, not semantic."
        ),
    ]
    uid: Annotated[
        str, Field(description="Globally unique identifier for this Stream Resource")
    ]
    stream_names: Annotated[
        List[str],
        Field(
            description="List of the stream names this resource provides",
            min_items=1,
            unique_items=True,
        ),
    ]
