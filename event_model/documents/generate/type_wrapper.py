"""
A wrapper used to patch out schema generation utilities.
"""


from dataclasses import dataclass

try:
    import pydantic

    Field = pydantic.Field
    FieldInfo = pydantic.fields.FieldInfo
except ModuleNotFoundError:

    def Field(*args, **kwargs):  # type: ignore
        ...

    class FieldInfo:  # type: ignore
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
