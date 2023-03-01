from typing import (
    Dict,
    List,
    Union,
)

from ._type_wrapper import Field, Annotated, Optional
from typing import TypedDict


class DatumPageOptional(TypedDict, total=False):
    datum_kwargs: Annotated[
        Optional[Dict[str, Union[bool, str, float, List]]],
        Field(
            description="Array of arguments to pass to the Handler to retrieve one quanta of data"
        ),
    ]


class DatumPage(DatumPageOptional):
    """Page of documents to reference a quanta of externally-stored data"""

    resource: Annotated[
        str,
        Field(
            description="The UID of the Resource to which all Datums in the page belong"
        ),
    ]
    datum_id: Annotated[
        List[str],
        Field(
            description="Array unique identifiers for each Datum (akin to 'uid' for other Document types), "
            "typically formatted as '<resource>/<integer>'"
        ),
    ]
