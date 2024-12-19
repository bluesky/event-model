# type: ignore

# Test schema generation
import json
from pathlib import Path

import pydantic
import pytest

from event_model.basemodels import ALL_BASEMODELS
from event_model.generate.create_documents import JSONSCHEMA, generate_jsonschema


@pytest.mark.parametrize("basemodel", ALL_BASEMODELS)
def test_generated_json_matches_typed_dict(basemodel, tmpdir: Path):
    tmp_documents = Path(tmpdir) / "documents"
    tmp_documents.mkdir()
    tmp_jsonschema = Path(tmpdir) / "jsonschema"
    tmp_jsonschema.mkdir()

    with pytest.warns(pydantic.warnings.PydanticDeprecatedSince20):
        generate_jsonschema(
            basemodel,
            jsonschema_parent_path=tmp_jsonschema,
            documents_parent_path=tmp_documents,
        )
    for new_jsonschema_path in tmp_jsonschema.iterdir():
        old_jsonschema_path = JSONSCHEMA / new_jsonschema_path.name

        if not old_jsonschema_path.exists():
            continue

        with new_jsonschema_path.open() as generated_file:
            with old_jsonschema_path.open() as old_file:
                try:
                    assert json.load(generated_file) == json.load(old_file)
                except AssertionError as error:
                    raise Exception(
                        f"`{basemodel.__name__}` can generate a json schema, but "
                        f"it doesn't match the schema in `{JSONSCHEMA}`. Did you "
                        "forget to run `regenerate-documents` after changes "
                        f"to `{basemodel.__name__}`?"
                    ) from error
