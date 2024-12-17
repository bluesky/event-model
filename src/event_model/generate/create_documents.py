import importlib
import importlib.util
import inspect
import json
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List

import datamodel_code_generator

from event_model.basemodels import ALL_BASEMODELS

from .type_wrapper import BaseModel, to_snake

JSONSCHEMA = Path(__file__).parent.parent / "jsonschemas"
DOCUMENTS = Path(__file__).parent.parent / "documents"


def sort_alphabetically(schema: Dict) -> Dict:
    """Sorts the schema alphabetically by key name, exchanging the
    properties dicts for OrderedDicts"""
    schema = OrderedDict(sorted(schema.items(), key=lambda x: x[0]))

    return schema


SortOrder = {
    "title": 0,
    "description": 1,
    "type": 2,
    "$defs": 3,
    "properties": 4,
    "required": 5,
    "additionalProperties": 6,
    "patternProperties": 7,
}


def sort_schema(document_schema: Dict) -> Dict:
    assert isinstance(document_schema, dict)
    document_schema = OrderedDict(
        sorted(
            document_schema.items(),
            key=lambda x: SortOrder.get(x[0], len(SortOrder)),
        )
    )

    for key in document_schema:
        if key in ("$defs", "properties", "required"):
            if isinstance(document_schema[key], dict):
                document_schema[key] = sort_alphabetically(document_schema[key])
                for key2 in document_schema[key]:
                    if isinstance(document_schema[key][key2], dict):
                        document_schema[key][key2] = sort_schema(
                            document_schema[key][key2]
                        )
            elif isinstance(document_schema[key], list):
                document_schema[key].sort()

    return document_schema


def dump_json(dictionary: Dict, directory=JSONSCHEMA):
    with open(directory / f"{to_snake(dictionary['title'])}.json", "w") as f:
        json.dump(dictionary, f, indent=4)


def import_basemodels(path: Path) -> List[BaseModel]:  # type: ignore
    # Dynamically import the module
    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to import {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return [
        attribute
        for attr_str in dir(module)
        if inspect.isclass(attribute := getattr(module, attr_str))
        and issubclass(attribute, BaseModel)
        and attribute != BaseModel
    ]


def generate_typeddict(json_schema_path: Path):
    datamodel_code_generator.generate(
        input_=json_schema_path,
        input_file_type=datamodel_code_generator.InputFileType.JsonSchema,
        output=DOCUMENTS / f"{json_schema_path.stem}.py",
        output_model_type=datamodel_code_generator.DataModelType.TypingTypedDict,
        use_schema_description=True,
        use_field_description=True,
        use_annotated=True,
        field_constraints=True,
    )


def generate_json_schema(basemodel: BaseModel, directory=JSONSCHEMA) -> List[BaseModel]:
    return_basemodels = []
    dump_json(basemodel.model_json_schema(), directory=directory)  # type: ignore
    return_basemodels.append(basemodel)

    for parent in [parent for parent in basemodel.__bases__ if parent is not BaseModel]:
        return_basemodels += generate_json_schema(parent)
    return return_basemodels


def generate():
    generated_basemodels = []
    for basemodel in ALL_BASEMODELS:
        generated_basemodels += generate_json_schema(basemodel)  # type: ignore
    for schema_path in JSONSCHEMA.iterdir():
        generate_typeddict(schema_path)

    init_py_imports = "\n".join(
        [
            f"from .{to_snake(basemodel.__name__)} import *"
            for basemodel in generated_basemodels
        ]
    )

    with open(DOCUMENTS / "__init__.py", "w") as f:
        f.write(init_py_imports)
