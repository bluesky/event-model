# Typed dicts to test generation of
from event_model.document_typed_dicts.datum_page import DatumPage
from event_model.document_typed_dicts.datum import Datum
from event_model.document_typed_dicts.event_descriptor import EventDescriptor
from event_model.document_typed_dicts.event_page import EventPage
from event_model.document_typed_dicts.event import Event
from event_model.document_typed_dicts.resource import Resource
from event_model.document_typed_dicts.run_start import RunStart, RUN_START_EXTRA_SCHEMA
from event_model.document_typed_dicts.run_stop import RunStop
from event_model.document_typed_dicts.stream_datum import StreamDatum
from event_model.document_typed_dicts.stream_resource import StreamResource

from event_model.typeddict_to_schema import parse_typeddict_to_schema
from event_model.__init__ import SCHEMA_PATH
import json
import pytest
import os

typed_dict_class_list = [
    DatumPage,
    Datum,
    EventDescriptor,
    EventPage,
    Event,
    Resource,
    RunStart,
    RunStop,
    StreamDatum,
    StreamResource,
]
extra_schema_list = [
    {},
    {},
    {},
    {},
    {},
    {},
    RUN_START_EXTRA_SCHEMA,
    {},
    {},
    {},
]

SCHEMA_PATH = "event_model/" + SCHEMA_PATH


@pytest.mark.parametrize(
    "typed_dict_class, extra_schema", zip(typed_dict_class_list, extra_schema_list)
)
def test_generated_json_matches_typed_dict(typed_dict_class, extra_schema, tmpdir):
    parse_typeddict_to_schema(
        typed_dict_class, out_dir=tmpdir, extra_schema=extra_schema
    )
    file_name = os.listdir(tmpdir)[0]
    generated_file_path = os.path.join(tmpdir, file_name)
    old_file_path = os.path.join(SCHEMA_PATH, file_name)

    with open(generated_file_path) as generated_file, open(old_file_path) as old_file:
        try:
            assert json.load(generated_file) == json.load(old_file)
        except AssertionError:
            raise Exception(
                f"`{typed_dict_class.__name__}` can generate a json schema, but "
                f"it doesn't match the schema in `{SCHEMA_PATH}`. Did you forget "
                "to run `python event_model/typeddict_to_schema.py` after changes "
                f"to `{typed_dict_class.__name__}`?"
            )
