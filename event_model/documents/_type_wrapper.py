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


if sys.version_info[:2] == (3, 8):
    from typing_extensions import Annotated as Annotated

    if typing.TYPE_CHECKING:
        from typing_extensions import NotRequired as Optional
    else:
        from typing_extensions import Optional as Optional

else:
    from typing import Annotated as Annotated

    if typing.TYPE_CHECKING:
        from typing_extensions import NotRequired as Optional
    else:
        from typing import Optional as Optional


extra_schema = {}


def add_extra_schema(schema):
    def inner(cls):
        extra_schema[cls] = schema
        return cls

    return inner
