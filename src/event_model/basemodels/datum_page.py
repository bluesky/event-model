from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field, RootModel
from typing_extensions import Annotated


class DataFrameForDatumPage(RootModel):
    root: List[str] = Field(alias="Dataframe")


class DatumPage(BaseModel):
    """Page of documents to reference a quanta of externally-stored data"""

    model_config = ConfigDict(extra="forbid")

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
