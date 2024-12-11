from pathlib import Path

import pytest
from pydantic.warnings import PydanticDeprecatedSince20

from event_model.generate.create_documents import (
    BASEMODELS,
    JSONSCHEMA,
    TYPEDDICTS,
    generate,
)


def test_generated_json_matches_typed_dict(tmpdir):
    tmpdir = Path(tmpdir)
    tmp_basemodels = tmpdir / "basemodels"
    tmp_typeddicts = tmpdir / "typeddicts"
    tmp_basemodels.mkdir()
    tmp_typeddicts.mkdir()

    with pytest.warns(PydanticDeprecatedSince20):
        generate(
            jsonschema_root=JSONSCHEMA,
            basemodel_root=tmp_basemodels,
            typeddict_root=tmp_typeddicts,
        )

    for new_basemodel in tmp_basemodels.iterdir():
        if new_basemodel.name == "__init__.py":
            continue
        old_basemodel = BASEMODELS / new_basemodel.name

        if (
            not old_basemodel.exists()
            or old_basemodel.read_text() != new_basemodel.read_text()
        ):
            raise RuntimeError(
                f"BaseModel {old_basemodel} is out of date with the schema, "
                "did you forget to run `regenerate-documents`?"
            )

    for new_typeddict in tmp_typeddicts.iterdir():
        if new_typeddict.name == "__init__.py":
            continue
        old_typeddict = TYPEDDICTS / new_typeddict.name

        if (
            not old_typeddict.exists()
            or old_typeddict.read_text() != new_typeddict.read_text()
        ):
            raise RuntimeError(
                f"Document {old_typeddict} is out of date with the schema, "
                "did you forget to run `regenerate-documents`?"
            )
