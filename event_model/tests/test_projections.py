import event_model
import pytest
from jsonschema.exceptions import ValidationError


@pytest.fixture
def start():
    run_bundle = event_model.compose_run()
    return run_bundle.start_doc


def test_projection_in_start_doc():
    run_bundle = event_model.compose_run(uid="42", metadata={"projections": valid_projections})
    start_doc = run_bundle.start_doc
    assert start_doc["projections"] == valid_projections


def test_projection_schema(start):
    start["projections"] = valid_projections
    event_model.schema_validators[event_model.DocumentNames.start].validate(start)

    with pytest.raises(ValidationError):
        start["projections"] = invalid_projections
        event_model.schema_validators[event_model.DocumentNames.start].validate(start)

    invalid_calc_projections = valid_projections.copy()
    invalid_calc_projections[0]["projection"]["calc_field"] = {"linked_field": {
                                                        "type": "linked",
                                                        "location": "event",
                                                        "stream": "primary",
                                                        "field": "ccd",
                                                        }}  # calc requires the calc fields
    with pytest.raises(ValidationError, ):
        event_model.schema_validators[event_model.DocumentNames.start].validate(start)


valid_projections = [
            {
                "name": "test",
                "version": "42.0.0",
                "configuration": {},
                "projection": {
                    "linked_field": {
                        "type": "linked",
                        "location": "event",
                        "stream": "primary",
                        "field": "ccd",
                    },
                    "calc_field": {
                        "type": "calculated",
                        "location": "event",
                        "field": "calc_field",
                        "stream": "calc_stream",
                        "calculation": {
                            "callable": "pizza.order:slice",
                            "kwargs": {"toppings": "cheese"}
                        }
                    },
                    "config_field": {
                        "type": "linked",
                        "location": "configuration",
                        "config_index": 0,
                        "config_device": "camera",
                        "stream": "primary",
                        "field": "setting"
                    }
                },
            }
        ]

invalid_projections = [
                {
                    "name": "test",
                    "version": "42.0.0",
                    "configuration": {},
                    "projection": {
                        "entry/instrument/detector/data": {
                            "location": "THIS IS NOT VALID",
                            "stream": "primary",
                            "field": "ccd",
                        },
                    },
                }
            ]
