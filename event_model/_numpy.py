import json

import numpy


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
