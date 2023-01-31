from importlib.metadata import version

__version__ = version("event_model")
del version

__all__ = ["__version__"]
