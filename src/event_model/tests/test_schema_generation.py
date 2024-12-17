# type: ignore

# Test schema generation
import json
import os

import pytest

from event_model.basemodels import ALL_BASEMODELS
from event_model.generate.create_documents import JSONSCHEMA, generate_json_schema


@pytest.mark.parametrize("typed_dict_class", ALL_BASEMODELS)
def test_generated_json_matches_typed_dict(typed_dict_class, tmpdir):
    generate_json_schema(typed_dict_class, directory=tmpdir)
    file_name = os.listdir(tmpdir)[0]
    generated_file_path = os.path.join(tmpdir, file_name)
    old_file_path = JSONSCHEMA / file_name

    with open(generated_file_path) as generated_file, open(old_file_path) as old_file:
        try:
            assert json.load(generated_file) == json.load(old_file)
        except AssertionError as error:
            raise Exception(
                f"`{typed_dict_class.__name__}` can generate a json schema, but "
                f"it doesn't match the schema in `{JSONSCHEMA}`. Did you forget "
                "to run `python event_model/documents/generate` after changes "
                f"to `{typed_dict_class.__name__}`?"
            ) from error
