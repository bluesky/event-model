"""Helpers to coerce data to the correct container.

A "coercion funcion" is a hook that Filler can use to, for example, ensure
all the external data read in my handlers is an *actual* numpy array as
opposed to some other array-like such as h5py.Dataset or dask.array.Array,
or wrap every result is dask.array.from_array(...).

It has access to the handler_class as it is registered and to some state
provided by the Filler (more on that below). It is expected to return
something that is API-compatible with handler_class.  That might be
handler_class itself (a no-op), a subclass, or an altogether different class
with the same API. See example below.

The "state provided by the Filler", mentioned above is passed into the
coercion functions below as ``filler_state``. It is a namespace containing
information that may be useful for the coercion functions.  Currently, it has
``filler_state.descriptor`` and ``filler_state.key``. More may be added in
the future if the need arises. Ultimately, this is necessary because Resource
documents don't know the shape and dtype of the data that they reference.
That situation could be improved in the future; to some degree this is a
work-around.

As an implementation detail, the ``filler_state`` is a ``threading.local``
object to ensure that filling is thread-safe.

Third-party libraries can register custom coercion options via the
register_coercion function below. For example, databroker uses this to
register a 'delayed' option. This avoids introducing dependency on a specific
delayed-computation framework (e.g. dask) in event-model itself.

"""
import numpy

from ._errors import EventModelValueError


def as_is(handler_class, filler_state):
    "A no-op coercion function that returns handler_class unchanged."
    return handler_class


def force_numpy(handler_class, filler_state):
    "A coercion that makes handler_class.__call__ return actual numpy.ndarray."

    class Subclass(handler_class):
        def __call__(self, *args, **kwargs):
            raw_result = super().__call__(*args, **kwargs)
            result_as_array = numpy.asarray(raw_result)
            return result_as_array

    Subclass.__name__ = f"Subclassed{handler_class.__name__}"
    Subclass.__qualname__ = f"Subclassed{handler_class.__qualname__}"
    return Subclass


# maps coerce option to corresponding coercion function
_coercion_registry = {"as_is": as_is, "force_numpy": force_numpy}


def register_coercion(name, func, overwrite=False):
    """
    Register a new option for :class:`Filler`'s ``coerce`` argument.

    This is an advanced feature. See source code for comments and examples.

    Parameters
    ----------
    name : string
        The new value for ``coerce`` that will invoke this function.
    func : callable
        Expected signature::

            func(filler, handler_class) -> handler_class
    overwrite : boolean, optional
        False by default. Name collissions will raise ``EventModelValueError``
        unless this is set to ``True``.
    """

    if name in _coercion_registry and not overwrite:
        # If we are re-registering the same object, there is no problem.
        original = _coercion_registry[name]
        if original is func:
            return
        raise EventModelValueError(
            f"The coercion function {func} could not be registered for the "
            f"name {name} because {_coercion_registry[name]} is already "
            f"registered. Use overwrite=True to force it."
        )
    _coercion_registry[name] = func
