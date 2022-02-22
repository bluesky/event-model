"""Enums for Event Model."""

from enum import Enum


class DocumentNames(Enum):
    """Document names allowed with in the Event Model."""

    stop = "stop"
    start = "start"
    descriptor = "descriptor"
    event = "event"
    datum = "datum"
    resource = "resource"
    event_page = "event_page"
    datum_page = "datum_page"
    bulk_datum = "bulk_datum"  # deprecated
    bulk_events = "bulk_events"  # deprecated
