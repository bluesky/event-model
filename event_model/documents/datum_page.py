from typing import Dict, List, Union

from typing_extensions import Annotated, NotRequired, TypedDict

from .generate.type_wrapper import AsRef, Field, add_extra_schema

DATUM_PAGE_EXTRA_SCHEMA = {"additionalProperties": False}


@add_extra_schema(DATUM_PAGE_EXTRA_SCHEMA)
class DatumPage(TypedDict):
    """Page of documents to reference a quanta of externally-stored data"""

    datum_id: Annotated[
        List[str],
        AsRef("Dataframe"),
        Field(
            description="Array unique identifiers for each Datum (akin to 'uid' for "
            "other Document types), typically formatted as '<resource>/<integer>'"
        ),
    ]
    datum_kwargs: NotRequired[
        Annotated[
            Dict[str, Union[bool, str, float, List]],
            Field(
                description="Array of arguments to pass to the Handler to "
                "retrieve one quanta of data"
            ),
        ]
    ]
    resource: Annotated[
        str,
        Field(
            description="The UID of the Resource to which all Datums in the page belong"
        ),
    ]
