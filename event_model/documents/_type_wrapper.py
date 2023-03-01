"""
A wrapper used to differentiate types between python versions and schema generation vs runtime.
"""

import sys
import typing
import typing_extensions


if sys.version_info[:2] == (3, 8):
    Annotated: typing.TypeVar = typing_extensions.Annotated
else:
    Annotated: typing.TypeVar = typing.Annotated

if typing.TYPE_CHECKING:
    import pydantic

    Field = pydantic.Field
    Optional: typing.TypeVar = typing.Optional
else:

    def Field(
        *args: typing_extensions.Unpack, **kwargs: typing_extensions.Unpack
    ) -> typing.Any:
        return

    Optional: typing.TypeVar = typing_extensions.NotRequired


extra_schema = {}


def add_extra_schema(schema):
    def inner(cls):
        extra_schema[cls] = schema
        return cls

    return inner
