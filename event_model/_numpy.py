import json
import typing
import numpy

from ._errors import EventModelValueError


def sanitize_doc(doc):
    """Return a copy with any numpy objects converted to built-in Python types.

    This function takes in an event-model document and returns a copy with any
    numpy objects converted to built-in Python types. It is useful for
    sanitizing documents prior to sending to any consumer that does not
    recognize numpy types, such as a MongoDB database or a JSON encoder.

    Parameters
    ----------
    doc : dict
        The event-model document to be sanitized

    Returns
    -------
    sanitized_doc : event-model document
        The event-model document with numpy objects converted to built-in
        Python types.
    """
    return json.loads(json.dumps(doc, cls=NumpyEncoder))


class NumpyEncoder(json.JSONEncoder):
    """
    A json.JSONEncoder for encoding numpy objects using built-in Python types.

    Examples
    --------

    Encode a Python object that includes an arbitrarily-nested numpy object.

    >>> json.dumps({'a': {'b': numpy.array([1, 2, 3])}}, cls=NumpyEncoder)
    """

    # Credit: https://stackoverflow.com/a/47626762/1221924
    def default(self, obj):
        try:
            import dask.array

            if isinstance(obj, dask.array.Array):
                obj = numpy.asarray(obj)
        except ImportError:
            pass
        if isinstance(obj, (numpy.generic, numpy.ndarray)):
            if numpy.isscalar(obj):
                return obj.item()
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def infer_datakeys(val):
    """
    Given a value, infer what the datatype (as Ewent Model would describe it).

    Parameters
    ----------
    val : Any

    """
    bad_iterables = (str, bytes, dict)
    _type_map = {
        "number": (float, numpy.floating, complex),
        "array": (numpy.ndarray, list, tuple),
        "string": (str,),
        "integer": (int, numpy.integer),
    }

    if isinstance(val, typing.Iterable) and not isinstance(val, bad_iterables):
        dtype = "array"
    else:
        for json_type, py_types in _type_map.items():
            if isinstance(val, py_types):
                dtype = json_type
                break
        else:
            raise EventModelValueError(
                f"Cannot determine the appropriate bluesky-friendly data type for "
                f"value {val} of Python type {type(val)}. "
                f"Supported types include: int, float, str, and iterables such as "
                f"list, tuple, np.ndarray, and so on."
            )

    # this should only make a copy if it _has to_.  If we have lots of
    # non-already-numpy arrays flowing through and this is doing things like
    # computing huge dask arrays etc.
    arr_val = numpy.asanyarray(val)
    arr_dtype = arr_val.dtype

    return {
        "dtype": dtype,
        "dtype_str": arr_dtype.str,
        "dtype_descr": arr_dtype.descr,
        "shape": list(arr_val.shape),
    }
