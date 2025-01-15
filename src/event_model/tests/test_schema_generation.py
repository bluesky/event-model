# type: ignore

from pathlib import Path

import pytest
from pydantic.warnings import PydanticDeprecatedSince20

from event_model.generate.create_documents import JSONSCHEMA, TYPEDDICTS, generate


def test_generated_json_matches_typed_dict(tmpdir: Path):
    tmp_documents = Path(tmpdir) / "documents"
    tmp_documents.mkdir()
    tmp_jsonschema = Path(tmpdir) / "jsonschema"
    tmp_jsonschema.mkdir()

    with pytest.warns(PydanticDeprecatedSince20):
        generate(jsonschema_root=tmp_jsonschema, documents_root=tmp_documents)

    for new_jsonschema_path in tmp_jsonschema.iterdir():
        old_jsonschema_path = JSONSCHEMA / new_jsonschema_path.name

        if (
            not old_jsonschema_path.exists()
            or new_jsonschema_path.read_text() != old_jsonschema_path.read_text()
        ):
            raise Exception(
                f"{str(old_jsonschema_path)} does not match "
                f"{str(new_jsonschema_path)}. Did you forget to run "
                "`python -m event_model.generate` after changes?"
            )

    for new_document_path in tmp_documents.iterdir():
        old_document_path = TYPEDDICTS / new_document_path.name

        if (
            not old_document_path.exists()
            or new_document_path.read_text() != old_document_path.read_text()
        ):
            raise Exception(
                f"{str(old_document_path)} does not match "
                f"{str(new_document_path)}. Did you forget to run "
                "`python -m event_model.generate` after changes?"
            )
