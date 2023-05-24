from uuid import uuid4

import jsonschema
import pytest
from jsonschema.exceptions import ValidationError

import event_model
from event_model import DocumentNames, schema_validators


def new_uid():
    return str(uuid4())


def test_data_session():
    run_bundle = event_model.compose_run(
        uid="42",
        metadata={"data_groups": ["a", "b"], "data_session": "magrathia_visit_42"},
    )
    start_doc = run_bundle.start_doc
    assert start_doc["data_groups"] == ["a", "b"]

    with pytest.raises(ValidationError):
        event_model.compose_run(
            uid="42", metadata={"data_session": ["shoud", "not", "be", "a", "list"]}
        )

    with pytest.raises(ValidationError):
        event_model.compose_run(
            uid="42", metadata={"data_groups": "this should be an array"}
        )


def test_dots_not_allowed_in_keys():
    doc = {"time": 0, "uid": new_uid()}
    schema_validators[DocumentNames.start].validate(doc)

    # Add a legal key.
    doc.update({"b": "c"})
    schema_validators[DocumentNames.start].validate(doc)
    # Now add illegal key.
    doc.update({"b.": "c"})
    with pytest.raises(jsonschema.ValidationError):
        schema_validators[DocumentNames.start].validate(doc)

    doc = {
        "time": 0,
        "uid": new_uid(),
        "data_keys": {"a": {"source": "", "dtype": "number", "shape": []}},
        "run_start": new_uid(),
    }
    schema_validators[DocumentNames.descriptor].validate(doc)
    # Add a legal key.
    doc.update({"b": "c"})
    schema_validators[DocumentNames.descriptor].validate(doc)
    # Now add illegal key.
    doc.update({"b.c": "d"})
    with pytest.raises(jsonschema.ValidationError):
        schema_validators[DocumentNames.descriptor].validate(doc)

    doc = {
        "time": 0,
        "uid": new_uid(),
        "exit_status": "success",
        "reason": "",
        "run_start": new_uid(),
    }
    schema_validators[DocumentNames.stop].validate(doc)
    # Add a legal key.
    doc.update({"b": "c"})
    schema_validators[DocumentNames.stop].validate(doc)
    # Now add illegal key.
    doc.update({".b": "c"})
    with pytest.raises(jsonschema.ValidationError):
        schema_validators[DocumentNames.stop].validate(doc)
