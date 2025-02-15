# ruff: noqa
# generated by datamodel-codegen:
#   filename:  stream_datum.json

from __future__ import annotations

from typing import TypedDict


class StreamRange(TypedDict):
    """
    The parameters required to describe a sequence of incrementing integers
    """

    start: int
    """
    First number in the range
    """
    stop: int
    """
    Last number in the range is less than this number
    """


class StreamDatum(TypedDict):
    """
    Document to reference a quanta of an externally-stored stream of data.
    """

    descriptor: str
    """
    UID of the EventDescriptor to which this Datum belongs
    """
    indices: StreamRange
    """
    A slice object passed to the StreamResource handler so it can hand back data and timestamps
    """
    seq_nums: StreamRange
    """
    A slice object showing the Event numbers the resource corresponds to
    """
    stream_resource: str
    """
    The UID of the Stream Resource to which this Datum belongs.
    """
    uid: str
    """
    Globally unique identifier for this Datum. A suggested formatting being '<stream_resource>/<stream_name>/<block_id>
    """
