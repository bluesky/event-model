# type: ignore

import sys

import pytest

if sys.version_info[:2] >= (3, 9):
    # Test schema generation
    import json
    import os

    from event_model import SCHEMA_PATH
    from event_model.documents import (
        Datum,
        DatumPage,
        Event,
        EventDescriptor,
        EventPage,
        Resource,
        RunStart,
        RunStop,
        StreamDatum,
        StreamResource,
    )
    from event_model.documents.generate.typeddict_to_schema import (
        parse_typeddict_to_schema,
    )

    typed_dict_class_list = [
        DatumPage,
        Datum,
        EventDescriptor,
        EventPage,
        Event,
        Resource,
        RunStart,
        RunStop,
        StreamDatum,
        StreamResource,
    ]

    SCHEMA_PATH = "event_model/" + SCHEMA_PATH

    @pytest.mark.parametrize("typed_dict_class", typed_dict_class_list)
    def test_generated_json_matches_typed_dict(typed_dict_class, tmpdir):
        parse_typeddict_to_schema(typed_dict_class, out_dir=tmpdir)
        file_name = os.listdir(tmpdir)[0]
        generated_file_path = os.path.join(tmpdir, file_name)
        old_file_path = os.path.join(SCHEMA_PATH, file_name)

        with open(generated_file_path) as generated_file, open(
            old_file_path
        ) as old_file:
            try:
                assert json.load(generated_file) == json.load(old_file)
            except AssertionError:
                raise Exception(
                    f"`{typed_dict_class.__name__}` can generate a json schema, but "
                    f"it doesn't match the schema in `{SCHEMA_PATH}`. Did you forget "
                    "to run `python event_model/typeddict_to_schema.py` after changes "
                    f"to `{typed_dict_class.__name__}`?"
                )

else:
    # Test an error is thrown for pyton <= 3.8
    def test_schema_generation_import_throws_error():
        with pytest.raises(EnvironmentError):
            from event_model.documents.generate import typeddict_to_schema

            typeddict_to_schema
