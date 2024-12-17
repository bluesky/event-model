# type: ignore
"""
A wrapper used to patch out schema generation utilities.
"""

from typing import Any, Dict, List, TypedDict, Union

pydantic_version = None
# Dictionary for patching in schema post generation
try:
    import pydantic

    if pydantic.__version__ < "2":
        raise ImportError

    pydantic_version = pydantic.__version__

    # Root models for root definitions:
    # we want some types to reference definitions in the
    # schema
    from pydantic import (
        BaseModel,
        ConfigDict,
        Field,
        RootModel,
        TypeAdapter,
        ValidationError,
        model_validator,
    )
    from pydantic.alias_generators import to_snake

    class DataFrameForDatumPage(RootModel):
        root: List[str] = Field(alias="Dataframe")

    class DataFrameForEventPage(RootModel):
        root: Dict[str, List] = Field(alias="Dataframe")

    class DataFrameForFilled(RootModel):
        root: Dict[str, List[Union[bool, str]]] = Field(alias="DataframeForFilled")

    class DataType(RootModel):
        root: Any = Field(alias="DataType")


# If pydantic is not installed (e.g the install isn't [dev]),
# or pydantic v1 is being used, then we expect to be able to
# run event-model, just not the schema generation code.
except (ModuleNotFoundError, ImportError):
    # None of the dummy functions/classes should have been overwritten.
    assert pydantic_version is None

    def Field(*args, **kwargs): ...

    class ConfigDict(TypedDict): ...

    def model_validator(*args, **kwargs):
        def inner(func):
            return func

        return inner

    class BaseModel:
        __fields__: Dict[str, Any]

        def __init__(self, *args, **kwargs):
            raise ImportError("pydantic is not installed")

    class RootModel(BaseModel):
        def __init__(self, *args, **kwargs):
            raise ImportError("pydantic is not installed")

    class TypeAdapter:
        def __init__(self, *args, **kwargs):
            self.by_alias = True
            ...

        def json_schema(self, *args, **kwargs): ...

    def to_snake(*args, **kwargs): ...

    ValidationError = Exception

    DataFrameForDatumPage = List[str]

    DataFrameForEventPage = Dict[str, List]

    DataFrameForFilled = Dict[str, List[Union[bool, str]]]

    class DataType: ...


__all__ = [
    "BaseModel",
    "ConfigDict",
    "DataFrameForDatumPage",
    "DataFrameForEventPage",
    "DataFrameForFilled",
    "DataType",
    "Field",
    "RootModel",
    "TypeAdapter",
    "ValidationError",
    "model_validator",
    "to_snake",
]
