import pytest
import numpy as np


from event_model._numpy import infer_datakeys
from event_model._errors import EventModelValueError


@pytest.mark.parametrize("shape", [[1], [2, 2]])
@pytest.mark.parametrize(
    "dtype", ["i8", "f2", "c16", np.dtype([("a", "i"), ("b", "f")])]
)
def test_infer_dtypes_array(shape, dtype):
    v = np.ones(shape, dtype=dtype)
    if isinstance(dtype, str):
        dtype = np.dtype(dtype)

    ret = infer_datakeys(v)

    assert ret["dtype"] == "array"
    assert ret["shape"] == list(shape)
    assert ret["dtype_str"] == dtype.str
    assert ret["dtype_descr"] == dtype.descr


@pytest.mark.parametrize("val", [{}, b"bob"])
def test_infer_fail(val):
    with pytest.raises(EventModelValueError):
        infer_datakeys(val)


@pytest.mark.parametrize(
    "value,dtype",
    [("bob", "string"), (1, "integer"), (1.0, "number"), (1 + 1j, "number")],
)
def test_infer_dtypes_scalar(value, dtype):

    ret = infer_datakeys(value)
    np_dt = np.array(value).dtype
    assert ret["dtype"] == dtype
    assert ret["shape"] == []

    assert ret["dtype_str"] == np_dt.str
    assert ret["dtype_descr"] == np_dt.descr
