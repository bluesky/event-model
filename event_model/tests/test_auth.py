import event_model
import pytest
from jsonschema.exceptions import ValidationError


def test_auth_session():
    run_bundle = event_model.compose_run(uid="42", metadata={"data_session": ['a', 'b']})
    start_doc = run_bundle.start_doc
    assert start_doc["data_session"] == ['a', 'b']

    with pytest.raises(ValidationError):
        run_bundle = event_model.compose_run(uid="42", metadata={"data_session": 42})
    with pytest.raises(ValidationError):
        run_bundle = event_model.compose_run(uid="42", metadata={"data_session": {"a": "b"}})
