import event_model
import pytest
from jsonschema.exceptions import ValidationError

def test_projection_start_doc():
    metadata = {'so_long': 'thanks_for_all_the_fish'}
    # proejctions are added to metadata which is added to start document, so
    # we have several paths to test here

    # 1 add proection without metadata
    run_bundle = event_model.compose_run(uid="42", projections=valid_projections)
    start_doc = run_bundle.start_doc 
    assert start_doc['projections'] == valid_projections

    # 2 add no projection    
    run_bundle = event_model.compose_run(uid="42", metadata=metadata)
    start_doc = run_bundle.start_doc 
    assert 'projections' not in start_doc
    assert start_doc['so_long'] == 'thanks_for_all_the_fish'


    # 3 add projection with  metadata
    run_bundle = event_model.compose_run(uid="42", metadata=metadata, projections=valid_projections)
    start_doc = run_bundle.start_doc 
    assert start_doc['projections'] == valid_projections
    assert start_doc['so_long'] == 'thanks_for_all_the_fish'


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
                        'location': 'event', 
                        'stream': 'primary', 
                        'field': 'ccd',
                        'slice_args': ['sdfsdfds']
                    },
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
                            'slice_args': ['sdfsdfds', 1]
                        },
                    },
                }
            ]
start_doc = {
    "uid": "abc",
    "time": 0,
} 