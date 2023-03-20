from typing import Dict, List, Union

from typing_extensions import Annotated, NotRequired, TypedDict

from ._type_wrapper import Field


class DatumPage(TypedDict):
    """Page of documents to reference a quanta of externally-stored data"""

    datum_kwargs: NotRequired[
        Annotated[
            Dict[str, Union[bool, str, float, List]],
            Field(
                description="Array of arguments to pass to the Handler to retrieve one quanta of data"
            ),
        ]
    ]

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
