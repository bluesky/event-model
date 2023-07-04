"""
A wrapper used to patch out schema generation utilities.
"""


from dataclasses import dataclass

pydantic_version = None
try:
    try:
        from pydantic import v1 as pydantic  # type: ignore
    except ImportError:
        import pydantic

    pydantic_version = pydantic.__version__

    Field = pydantic.Field
    FieldInfo = pydantic.fields.FieldInfo
    BaseConfig = pydantic.BaseConfig
    BaseModel = pydantic.BaseModel
    create_model = pydantic.create_model
except ModuleNotFoundError:

    def Field(*args, **kwargs):  # type: ignore
        ...

    class FieldInfo:  # type: ignore
        ...

    class BaseConfig:  # type: ignore
        ...

    class BaseModel:  # type: ignore
        ...

    def create_model(*args, **kwargs):  # type: ignore
        ...


extra_schema = {}


def add_extra_schema(schema: dict):
    def inner(cls):
        extra_schema[cls] = schema
        return cls

    return inner


@dataclass
class AsRef:
    ref_name: str


# We need to check that only one element in an annotation is the type of
# the field, the others have to be instances of classes in this tuple
ALLOWED_ANNOTATION_ELEMENTS = (AsRef, FieldInfo)
