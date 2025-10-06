from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class Datum(BaseModel):
    """Document to reference a quanta of externally-stored data"""

    model_config = ConfigDict(extra="forbid")

    datum_id: Annotated[
        str,
        Field(
            description="Globally unique identifier for this Datum (akin to 'uid' "
            "for other Document types), typically formatted as '<resource>/<integer>'"
        ),
    ]
    datum_kwargs: Annotated[
        dict[str, Any],
        Field(
            description="Arguments to pass to the Handler to "
            "retrieve one quanta of data",
        ),
    ]
    resource: Annotated[
        str, Field(description="The UID of the Resource to which this Datum belongs")
    ]
