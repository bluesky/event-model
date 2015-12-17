import json
from enum import Enum
from pkg_resources import resource_filename as rs_fn


__all__ = ['DocumentNames', 'schemas']


class DocumentNames(Enum):
    stop = 'stop'
    start = 'start'
    descriptor = 'descriptor'
    event = 'event'
    bulk_events = 'bulk_events'


SCHEMA_PATH = 'schemas'
SCHEMA_NAMES = {DocumentNames.start: 'schemas/run_start.json',
                DocumentNames.stop: 'schemas/run_stop.json',
                DocumentNames.event: 'schemas/event.json',
                DocumentNames.bulk_events: 'schemas/bulk_events.json',
                DocumentNames.descriptor: 'schemas/event_descriptor.json'}
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    with open(rs_fn('event_model', filename)) as fin:
        schemas[name] = json.load(fin)

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
