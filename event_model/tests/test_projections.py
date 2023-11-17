import pytest
from jsonschema.exceptions import ValidationError

import event_model


@pytest.fixture
def start():
    run_bundle = event_model.compose_run()
    return run_bundle.start_doc


def test_projection_in_start_doc():
    run_bundle = event_model.compose_run(
        uid="42", metadata={"projections": valid_projections}
    )
    start_doc = run_bundle.start_doc
    assert start_doc["projections"] == valid_projections


def test_projection_schema(start):
    start["projections"] = valid_projections
    event_model.schema_validators[event_model.DocumentNames.start].validate(start)


def test_bad_calc_field(start):
    bad_calc_projections = [
        # calc requires the calc fields
        {
            "name": "test",
            "version": "42.0.0",
            "configuration": {},
            "projection": {
                "linked_field": {
                    "type": "calculated",
                    "location": "event",
                    "stream": "primary",
                    "field": "ccd",
                },
            },
        },
    ]

    start["projections"] = bad_calc_projections
    with pytest.raises(
        ValidationError,
    ):
        event_model.schema_validators[event_model.DocumentNames.start].validate(start)


def test_bad_configuration_field(start):
    bad_configuration_projections = [
        {
            "name": "test",
            "version": "42.0.0",
            "configuration": {},
            "projection": {
                "bad_config_field": {
                    "type": "calculated",
                    "location": "event",
                    "config_index": 0,
                    "config_device": "camera",
                    "stream": "primary",
                    # "field": "setting"
                },
            },
        },
    ]

    start["projections"] = bad_configuration_projections
    with pytest.raises(
        ValidationError,
    ):
        event_model.schema_validators[event_model.DocumentNames.start].validate(start)


def test_bad_event_field(start):
    bad_event_projections = [
        {
            "name": "test",
            "version": "42.0.0",
            "configuration": {},
            "projection": {
                "bad_event_field": {
                    "type": "linked",
                    "location": "event",
                    # "stream": "primary",
                    "field": "ccd",
                },
            },
        },
    ]
    start["projections"] = bad_event_projections
    with pytest.raises(
        ValidationError,
    ):
        event_model.schema_validators[event_model.DocumentNames.start].validate(start)


def test_bad_location_field(start):
    bad_event_projections = [
        {
            "name": "test",
            "version": "42.0.0",
            "configuration": {},
            "projection": {
                "bad_event_field": {
                    "type": "linked",
                    "location": "event",
                    # "stream": "primary",
                    "field": "ccd",
                },
            },
        },
    ]
    start["projections"] = bad_event_projections
    with pytest.raises(
        ValidationError,
    ):
        event_model.schema_validators[event_model.DocumentNames.start].validate(start)


def test_bad_static_field(start):
    bad_event_projections = [
        {
            "name": "test",
            "version": "42.0.0",
            "configuration": {},
            "projection": {
                "bad_event_field": {
                    "type": "static",
                    "location": "event",
                    # "stream": "primary",
                    "field": "ccd",
                },
            },
        },
    ]
    start["projections"] = bad_event_projections
    with pytest.raises(
        ValidationError,
    ):
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
                    "kwargs": {"toppings": "cheese"},
                },
            },
            "config_field": {
                "type": "linked",
                "location": "configuration",
                "config_index": 0,
                "config_device": "camera",
                "stream": "primary",
                "field": "setting",
            },
            "static_field": {"type": "static", "value": "strcvsdf"},
        },
    }
]
