"""
A wrapper used to differentiate types between python versions and schema generation vs runtime.
"""


import sys
import typing
import typing_extensions


# Annotated is in `typing_extensions` for python <3.9.
if sys.version_info[:2] >= (3, 9):
    Annotated = typing.Annotated
    TypedDict = typing.TypedDict
else:
    Annotated = typing_extensions.Annotated
    TypedDict = typing_extensions.TypedDict

if sys.version_info[:2] >= (3, 8):
    Literal = typing.Literal
    _TypedDictMeta = typing._TypedDictMeta
else:
    Literal = typing_extensions.Literal
    _TypedDictMeta = typing_extensions._TypedDictMeta

# Fields should do nothing at runtime when pydantic won't be installed or used.
# If the dev install has occured (pydantic has been installed) then use `pydantic.Field`.
# If the dev install has occured then use `typing.Optional` as Optional to generate the
# schema correctly, otherwise during runtime we use the `NotRequired` for those fields.
try:
    import pydantic

    Field = pydantic.Field
    Optional = typing.Optional
except ModuleNotFoundError:

    def Field(*args, **kwargs):
        ...

    Optional = typing_extensions.NotRequired
