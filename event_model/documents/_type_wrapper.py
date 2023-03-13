# flake8: noqa
"""
A wrapper used to differentiate types between python versions and schema generation vs runtime.

Imports of types take the form of:
from [typing, typing_extensions] import X as X

This is intentional and due to a quirk of how generic types are handled on assignment:
https://github.com/python/mypy/issues/10068#issuecomment-806256214
"""

import sys
import typing

try:
    import pydantic

    pydantic_installed = True
    Field = pydantic.Field

except ModuleNotFoundError:
    pydantic_installed = False

    def Field(*args, **kwargs):
        ...


extra_schema = {}


def add_extra_schema(schema):
    def inner(cls):
        extra_schema[cls] = schema
        return cls

    return inner
