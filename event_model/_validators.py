"""The core schema validatiors of EventModel."""

import types
from pkg_resources import resource_filename as rs_fn
import json
from distutils.version import LooseVersion
from functools import partial

import jsonschema

from ._enums import DocumentNames

SCHEMA_PATH = "schemas"
SCHEMA_NAMES = {
    DocumentNames.start: "schemas/run_start.json",
    DocumentNames.stop: "schemas/run_stop.json",
    DocumentNames.event: "schemas/event.json",
    DocumentNames.event_page: "schemas/event_page.json",
    DocumentNames.descriptor: "schemas/event_descriptor.json",
    DocumentNames.datum: "schemas/datum.json",
    DocumentNames.datum_page: "schemas/datum_page.json",
    DocumentNames.resource: "schemas/resource.json",
    # DEPRECATED:
    DocumentNames.bulk_events: "schemas/bulk_events.json",
    DocumentNames.bulk_datum: "schemas/bulk_datum.json",
}
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    with open(rs_fn("event_model", filename)) as fin:
        schemas[name] = json.load(fin)


# We pin jsonschema >=3.0.0 in requirements.txt but due to pip's dependency
# resolution it is easy to end up with an environment where that pin is not
# respected. Thus, we maintain best-effort support for 2.x.
if LooseVersion(jsonschema.__version__) >= LooseVersion("3.0.0"):

    def _is_array(checker, instance):
        return (
            jsonschema.validators.Draft7Validator.TYPE_CHECKER.is_type(
                instance, "array"
            )
            or isinstance(instance, tuple)
            or hasattr(instance, "__array__")
        )

    _array_type_checker = jsonschema.validators.Draft7Validator.TYPE_CHECKER.redefine(
        "array", _is_array
    )

    _Validator = jsonschema.validators.extend(
        jsonschema.validators.Draft7Validator, type_checker=_array_type_checker
    )

    schema_validators = {
        name: _Validator(schema=schema) for name, schema in schemas.items()
    }
else:
    # Make objects that mock the one method on the jsonschema 3.x
    # Draft7Validator API that we need.
    schema_validators = {
        name: types.SimpleNamespace(
            validate=partial(
                jsonschema.validate, schema=schema, types={"array": (list, tuple)}
            )
        )
        for name, schema in schemas.items()
    }
