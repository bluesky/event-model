from typing import Any, Dict, List

from typing_extensions import Annotated, TypedDict

from .generate.type_wrapper import DataFrameForDatumPage, Field, add_extra_schema

DATUM_PAGE_EXTRA_SCHEMA = {"additionalProperties": False}


@add_extra_schema(DATUM_PAGE_EXTRA_SCHEMA)
class DatumPage(TypedDict):
    """Page of documents to reference a quanta of externally-stored data"""

    datum_id: Annotated[
        DataFrameForDatumPage,
        Field(
            description="Array unique identifiers for each Datum (akin to 'uid' for "
            "other Document types), typically formatted as '<resource>/<integer>'"
        ),
    ]
    datum_kwargs: Annotated[
        Dict[str, List[Any]],
        Field(
            description="Array of arguments to pass to the Handler to "
            "retrieve one quanta of data"
        ),
    ]
    resource: Annotated[
        str,
        Field(
            description="The UID of the Resource to which all Datums in the page belong"
        ),
    ]
