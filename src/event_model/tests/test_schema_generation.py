# type: ignore

# Test schema generation
import json
import os

import pytest

import event_model
from event_model.documents import ALL_DOCUMENTS
from event_model.documents.generate.typeddict_to_schema import typeddict_to_schema

SCHEMA_PATH = event_model.__path__[0] + "/schemas/"


@pytest.mark.parametrize("typed_dict_class", ALL_DOCUMENTS)
def test_generated_json_matches_typed_dict(typed_dict_class, tmpdir):
    typeddict_to_schema(typed_dict_class, schema_dir=tmpdir)
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
                "to run `python event_model/documents/generate` after changes "
                f"to `{typed_dict_class.__name__}`?"
            )
