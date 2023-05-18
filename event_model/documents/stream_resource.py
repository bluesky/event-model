from typing import Any, Dict, List

from typing_extensions import Annotated, Literal, NotRequired, TypedDict

from .generate.type_wrapper import Field, add_extra_schema

STREAM_RESOURCE_EXTRA_SCHEMA = {"additionalProperties": False}

add_extra_schema(STREAM_RESOURCE_EXTRA_SCHEMA)


class StreamResource(TypedDict):
    """
    Document to reference a collection (e.g. file or group of files) of
    externally-stored data streams
    """

    data_keys: Annotated[
        List[str],
        Field(
            description="A list to show which data_keys of the "
            "Descriptor are being streamed"
        ),
    ]
    seq_nums: Annotated[
        Dict[str, int],
        Field(
            description="A slice object showing the Event numbers the "
            "resource corresponds to"
        ),
    ]
    indices: Annotated[
        Dict[str, int],
        Field(
            description="A slice object passed to the StreamResource "
            "handler so it can hand back data and timestamps"
        ),
    ]

    path_semantics: NotRequired[
        Annotated[
            Literal["posix", "windows"],
            Field(description="Rules for joining paths"),
        ]
    ]
    resource_kwargs: Annotated[
        Dict[str, Any],
        Field(
            description="Additional argument to pass to the Handler to read a "
            "Stream Resource",
        ),
    ]
    resource_path: Annotated[
        str, Field(description="Filepath or URI for locating this resource")
    ]
    root: Annotated[
        str,
        Field(
            description="Subset of resource_path that is a local detail, not semantic"
        ),
    ]
    run_start: NotRequired[
        Annotated[
            str,
            Field(
                description="Globally unique ID to the run_start document "
                "this Stream Resource is associated with",
            ),
        ]
    ]
    spec: Annotated[
        str,
        Field(
            description="String identifying the format/type of this Stream Resource, "
            "used to identify a compatible Handler",
        ),
    ]
    uid: Annotated[
        str, Field(description="Globally unique identifier for this Stream Resource")
    ]
