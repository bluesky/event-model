from typing import Any, Dict

from typing_extensions import Annotated, TypedDict

from .generate.type_wrapper import Field, add_extra_schema

DATUM_EXTRA_SCHEMA = {"additionalProperties": False}


@add_extra_schema(DATUM_EXTRA_SCHEMA)
class Datum(TypedDict):
    """Document to reference a quanta of externally-stored data"""

    datum_id: Annotated[
        str,
        Field(
            description="Globally unique identifier for this Datum (akin to 'uid' "
            "for other Document types), typically formatted as '<resource>/<integer>'"
        ),
    ]
    datum_kwargs: Annotated[
        Dict[str, Any],
        Field(
            description="Arguments to pass to the Handler to "
            "retrieve one quanta of data",
        ),
    ]
    resource: Annotated[
        str, Field(description="The UID of the Resource to which this Datum belongs")
    ]
