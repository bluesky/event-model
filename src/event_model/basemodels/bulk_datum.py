# ruff: noqa
# type: ignore
# generated by datamodel-codegen:
#   filename:  bulk_datum.json

from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict


class BulkDatum(BaseModel):
    """
    Document to reference a quanta of externally-stored data
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    datum_kwarg_list: List[Dict[str, Any]]
    """
    Array of arguments to pass to the Handler to retrieve one quanta of data
    """
    resource: str
    """
    UID of the Resource to which all these Datum documents belong
    """
    datum_ids: List[str]
    """
    Globally unique identifiers for each Datum (akin to 'uid' for other Document types), typically formatted as '<resource>/<integer>'
    """
