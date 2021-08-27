"""Exception classes for event model."""


class EventModelError(Exception):
    ...


# Here we define subclasses of all of the built-in Python exception types (as
# needed, not a comprehensive list) so that all errors raised *directly* by
# event_model also inhereit from EventModelError as well as the appropriate
# built-in type. This means, for example, that `EventModelValueError` can be
# caught by `except ValueError:` or by `except EventModelError:`. This can be
# useful for higher-level libraries and for debugging.


class EventModelKeyError(EventModelError, KeyError):
    ...


class EventModelValueError(EventModelError, ValueError):
    ...


class EventModelRuntimeError(EventModelError, RuntimeError):
    ...


class EventModelTypeError(EventModelError, TypeError):
    ...


class EventModelValidationError(EventModelError):
    ...


class UnfilledData(EventModelError):
    """raised when unfilled data is found"""

    ...


class UndefinedAssetSpecification(EventModelKeyError):
    """raised when a resource spec is missing from the handler registry"""

    ...


class DataNotAccessible(EventModelError, IOError):
    """raised when attempts to load data referenced by Datum document fail"""

    ...


class UnresolvableForeignKeyError(EventModelValueError):
    """when we see a foreign before we see the thing to which it refers"""

    def __init__(self, key, message):
        self.key = key
        self.message = message


class DuplicateHandler(EventModelRuntimeError):
    """raised when a handler is already registered for a given spec"""

    ...


class InvalidData(EventModelError):
    """raised when the data is invalid"""

    ...


class MismatchedDataKeys(InvalidData):
    """
    Raised when any data keys structures are out of sync. This includes,
    event['data'].keys(), descriptor['data_keys'].keys(),
    event['timestamp'].keys(), event['filled'].keys()
    """

    ...
