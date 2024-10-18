"""
A wrapper used to patch out schema generation utilities.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Union

pydantic_version = None


def Field(*args, **kwargs): ...


class BaseModel: ...


class TypeAdapter:
    def __init__(self, *args, **kwargs):
        self.by_alias = True
        ...

    def json_schema(self, *args, **kwargs): ...


class GenerateJsonSchema: ...


def to_snake(*args, **kwargs): ...


DataFrameForDatumPage = List[str]

DataFrameForEventPage = Dict[str, List]

DataFrameForFilled = Dict[str, List[Union[bool, str]]]


class DataType: ...


def add_extra_schema(*args, **kwargs):
    def inner(cls):
        return cls

    return inner


# Dictionary for patching in schema post generation
extra_schema = {}  # type: ignore

if not TYPE_CHECKING:
    try:
        import pydantic

        if pydantic.__version__ < "2":
            raise ImportError

        pydantic_version = pydantic.__version__

        # Root models for root definitions:
        # we want some types to reference definitions in the
        # schema
        BaseModel = pydantic.BaseModel
        ConfigDict = pydantic.ConfigDict
        Field = pydantic.Field
        RootModel = pydantic.RootModel
        TypeAdapter = pydantic.TypeAdapter
        GenerateJsonSchema = pydantic.json_schema.GenerateJsonSchema
        from pydantic.alias_generators import to_snake  # noqa

        class Config(ConfigDict): ...

        class DataFrameForDatumPage(RootModel):
            root: List[str] = Field(alias="Dataframe")

        class DataFrameForEventPage(RootModel):
            root: Dict[str, List] = Field(alias="Dataframe")

        class DataFrameForFilled(RootModel):
            root: Dict[str, List[Union[bool, str]]] = Field(alias="DataframeForFilled")

        class DataType(RootModel):
            root: Any = Field(alias="DataType")

        def add_extra_schema(schema: Dict) -> Dict:
            def inner(cls):
                extra_schema[cls] = schema
                return cls

            return inner

    # If pydantic is not installed (e.g the install isn't [dev]),
    # or pydantic v1 is being used, then we expect to be able to
    # run event-model, just not the schema generation code.
    except (ModuleNotFoundError, ImportError):
        # None of the dummy functions/classes should have been overwritten.
        assert pydantic_version is None
