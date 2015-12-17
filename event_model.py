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
SCHEMA_NAMES = {DocumentNames.start: 'run_start.json',
                DocumentNames.stop: 'run_stop.json',
                DocumentNames.event: 'event.json',
                DocumentNames.bulk_events: 'bulk_events.json',
                DocumentNames.descriptor: 'event_descriptor.json'}
fn = '{}/{{}}'.format(SCHEMA_PATH)
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    with open(rs_fn('schemas', fn.format(filename))) as fin:
        schemas[name] = json.load(fin)
