from pathlib import Path

import datamodel_code_generator

JSONSCHEMA = Path(__file__).parent.parent / "schemas"
TYPEDDICTS = Path(__file__).parent.parent / "documents"
BASEMODELS = Path(__file__).parent.parent / "basemodels"

CUSTOM_TEMPLATES = Path(__file__).parent / "custom_templates"


def prepend_ignore(path: Path):
    with path.open("r+") as f:
        content = f.read()
        f.seek(0, 0)
        f.write("# ruff: noqa\n# type: ignore\n" + content)


def generate_typeddict(jsonschema_path: Path, documents_path=TYPEDDICTS):
    output_path = documents_path / f"{jsonschema_path.stem}.py"
    datamodel_code_generator.generate(
        jsonschema_path,
        input_file_type=datamodel_code_generator.InputFileType.JsonSchema,
        output=output_path,
        output_model_type=datamodel_code_generator.DataModelType.TypingTypedDict,
        target_python_version=datamodel_code_generator.PythonVersion.PY_38,
        use_schema_description=True,
        use_field_description=True,
        field_constraints=True,
        disable_timestamp=True,
        use_double_quotes=True,
        custom_template_dir=CUSTOM_TEMPLATES,
    )
    prepend_ignore(output_path)


def generate_basemodel(jsonschema_path: Path, basemodel_root=BASEMODELS):
    basemodel_path = basemodel_root / f"{jsonschema_path.stem}.py"
    datamodel_code_generator.generate(
        jsonschema_path,
        input_file_type=datamodel_code_generator.InputFileType.JsonSchema,
        output=basemodel_path,
        output_model_type=datamodel_code_generator.DataModelType.PydanticV2BaseModel,
        target_python_version=datamodel_code_generator.PythonVersion.PY_38,
        use_schema_description=True,
        use_field_description=True,
        field_constraints=True,
        disable_timestamp=True,
        use_double_quotes=True,
        custom_template_dir=CUSTOM_TEMPLATES,
    )
    prepend_ignore(basemodel_path)


GENERATED_INIT_PY = """# generated in `event_model/generate`

from typing import Tuple, Type, Union

{0}

{1}Type = Union[
{2}
]

ALL_{3}: Tuple[{1}Type, ...] = (
{4}
)"""


def generate_init_py(output_root: Path):
    document_names = [
        file.stem
        for file in output_root.iterdir()
        if file.stem != "__init__" and file.suffix == ".py"
    ]

    document_class_names = [
        f"{document_name.title().replace('_', '')}" for document_name in document_names
    ]

    init_py_imports = "\n".join(
        sorted(
            [
                f"from .{document_name} import *  # noqa: F403"
                for document_name in document_names
            ]
        )
    )

    document_types = "\n".join(
        [
            f"    Type[{class_name}],  # noqa: F405,"
            for class_name in document_class_names
        ]
    )

    all_documents = "\n".join(
        [f"    {class_name},  # noqa: F405" for class_name in document_class_names]
    )

    init_py = GENERATED_INIT_PY.format(
        init_py_imports,
        output_root.name.rstrip("s").title(),
        document_types,
        output_root.name.upper(),
        all_documents,
    )

    with open(output_root / "__init__.py", "w") as f:
        f.write(init_py + "\n")


def generate(
    jsonschema_root: Path = JSONSCHEMA,
    typeddict_root: Path = TYPEDDICTS,
    basemodel_root: Path = BASEMODELS,
):
    for jsonschema in jsonschema_root.iterdir():
        generate_typeddict(jsonschema, documents_path=typeddict_root)
        generate_basemodel(jsonschema, basemodel_root=basemodel_root)

    generate_init_py(typeddict_root)
    generate_init_py(basemodel_root)
