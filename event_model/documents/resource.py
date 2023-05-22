from typing import Any, Dict

from typing_extensions import Annotated, Literal, NotRequired, TypedDict

from .generate.type_wrapper import Field, add_extra_schema

RESOURCE_EXTRA_SCHEMA = {"additionalProperties": False}


class PartialResource(TypedDict):
    """
    Fields seperated from the complete Resource for use by the protocols.
    """

    path_semantics: NotRequired[
        Annotated[
            Literal["posix", "windows"],
            Field(description="Rules for joining paths"),
        ]
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
    spec: Annotated[
        str,
        Field(
            description="String identifying the format/type of this Resource, used to "
            "identify a compatible Handler",
        ),
    ]
    resource_path: Annotated[
        str, Field(description="Filepath or URI for locating this resource")
    ]
    uid: Annotated[
        str, Field(description="Globally unique identifier for this Resource")
    ]


@add_extra_schema(RESOURCE_EXTRA_SCHEMA)
class Resource(PartialResource):
    """
    Document to reference a collection (e.g. file or group of files) of
    externally-stored data
    """

    run_start: NotRequired[
        Annotated[
            str,
            Field(
                description="Globally unique ID to the run_start document this "
                "resource is associated with.",
            ),
        ]
    ]
