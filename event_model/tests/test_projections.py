import event_model
import pytest
from jsonschema.exceptions import ValidationError


def test_projection_start_doc():
    run_bundle = event_model.compose_run(uid="42", metadata={"projections": valid_projections})
    start_doc = run_bundle.start_doc
    assert start_doc['projections'] == valid_projections


def test_projection_schema():
    start_doc['projections'] = valid_projections
    event_model.schema_validators[event_model.DocumentNames.start].validate(start_doc)

    with pytest.raises(ValidationError):
        start_doc['projections'] = invalid_projections
        event_model.schema_validators[event_model.DocumentNames.start].validate(start_doc)


valid_projections = [
            {
                "name": "test",
                "version": "42.0.0",
                "configuration": {},
                "projection": {
                    'entry/instrument/detector/data': {
                        'type': 'linked',
                        'location': 'event',
                        'stream': 'primary',
                        'field': 'ccd',
                    },
                    '/entry/instrument/wavelength': {
                        'type': 'calculated',
                        'calculation': {
                            'callable': 'pizza.order:slice',
                            'kwargs': {'toppings': 'cheese'}
                        }
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
                        'entry/instrument/detector/data': {
                            'location': 'THIS IS NOT VALID',
                            'stream': 'primary',
                            'field': 'ccd',
                        },
                    },
                }
            ]

start_doc = {
    "uid": "abc",
    "time": 0,
}
