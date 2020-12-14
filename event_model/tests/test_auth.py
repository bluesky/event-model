import event_model
import pytest
from jsonschema.exceptions import ValidationError


def test_data_session():
    run_bundle = event_model.compose_run(uid="42", metadata={"data_groups": ["a", "b"],
                                                             "data_session": "magrathia_visit_42"})
    start_doc = run_bundle.start_doc
    assert start_doc["data_groups"] == ['a', 'b']

    with pytest.raises(ValidationError):
        event_model.compose_run(uid="42", metadata={"data_session": ["shoud", "not", "be", "a", "list"]})

    with pytest.raises(ValidationError):
        event_model.compose_run(uid="42", metadata={"data_groups": "this should be an array"})
