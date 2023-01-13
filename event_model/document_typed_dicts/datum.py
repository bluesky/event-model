from typing import Any, Dict

from ._type_wrapper import Field, Annotated, TypedDict


class Datum(TypedDict):
    """Document to reference a quanta of externally-stored data"""

    datum_kwargs: Annotated[
        Dict[str, Any],
        Field(
            description="Arguments to pass to the Handler to retrieve one quanta of data",
        ),
    ]
    resource: Annotated[
        str, Field(description="The UID of the Resource to which this Datum belongs")
    ]
    datum_id: Annotated[
        str,
        Field(
            description="Globally unique identifier for this Datum (akin to 'uid' for other "
            "Document types), typically formatted as '<resource>/<integer>'"
        ),
    ]
