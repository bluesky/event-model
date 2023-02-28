from importlib.metadata import version

__version__ = version("event-model")
del version

__all__ = ["__version__"]
