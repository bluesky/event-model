from collections import namedtuple
import uuid
from functools import partial
import itertools
import time as ttime
import os

from ._enums import DocumentNames
from ._validators import schema_validators
from ._errors import EventModelError, EventModelValidationError

ComposeRunBundle = namedtuple(
    "ComposeRunBundle", "start_doc compose_descriptor compose_resource " "compose_stop"
)
ComposeDescriptorBundle = namedtuple(
    "ComposeDescriptorBundle", "descriptor_doc compose_event compose_event_page"
)
ComposeResourceBundle = namedtuple(
    "ComposeResourceBundle", "resource_doc compose_datum compose_datum_page"
)


def compose_datum(*, resource, counter, datum_kwargs, validate=True):
    resource_uid = resource["uid"]
    doc = {
        "resource": resource_uid,
        "datum_kwargs": datum_kwargs,
        "datum_id": "{}/{}".format(resource_uid, next(counter)),
    }
    if validate:
        schema_validators[DocumentNames.datum].validate(doc)
    return doc


def compose_datum_page(*, resource, counter, datum_kwargs, validate=True):
    resource_uid = resource["uid"]
    any_column, *_ = datum_kwargs.values()
    N = len(any_column)
    doc = {
        "resource": resource_uid,
        "datum_kwargs": datum_kwargs,
        "datum_id": ["{}/{}".format(resource_uid, next(counter)) for _ in range(N)],
    }
    if validate:
        schema_validators[DocumentNames.datum_page].validate(doc)
    return doc


default_path_semantics = {"posix": "posix", "nt": "windows"}[os.name]


def compose_resource(
    *,
    spec,
    root,
    resource_path,
    resource_kwargs,
    path_semantics=default_path_semantics,
    start=None,
    uid=None,
    validate=True,
):
    if uid is None:
        uid = str(uuid.uuid4())
    counter = itertools.count()
    doc = {
        "uid": uid,
        "spec": spec,
        "root": root,
        "resource_path": resource_path,
        "resource_kwargs": resource_kwargs,
        "path_semantics": path_semantics,
    }
    if start:
        doc["run_start"] = start["uid"]

    if validate:
        schema_validators[DocumentNames.resource].validate(doc)

    return ComposeResourceBundle(
        doc,
        partial(compose_datum, resource=doc, counter=counter),
        partial(compose_datum_page, resource=doc, counter=counter),
    )


def compose_stop(
    *,
    start,
    event_counter,
    poison_pill,
    exit_status="success",
    reason="",
    uid=None,
    time=None,
    validate=True,
):
    if poison_pill:
        raise EventModelError(
            "Already composed a RunStop document for run " "{!r}.".format(start["uid"])
        )
    poison_pill.append(object())
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    doc = {
        "uid": uid,
        "time": time,
        "run_start": start["uid"],
        "exit_status": exit_status,
        "reason": reason,
        "num_events": {k: v - 1 for k, v in event_counter.items()},
    }
    if validate:
        schema_validators[DocumentNames.stop].validate(doc)
    return doc


def compose_event_page(
    *,
    descriptor,
    event_counter,
    data,
    timestamps,
    seq_num,
    filled=None,
    uid=None,
    time=None,
    validate=True,
):
    N = len(seq_num)
    if uid is None:
        uid = [str(uuid.uuid4()) for _ in range(N)]
    if time is None:
        time = [ttime.time()] * N
    if filled is None:
        filled = {}
    doc = {
        "uid": uid,
        "time": time,
        "data": data,
        "timestamps": timestamps,
        "seq_num": seq_num,
        "filled": filled,
        "descriptor": descriptor["uid"],
    }
    if validate:
        schema_validators[DocumentNames.event_page].validate(doc)
        if not (descriptor["data_keys"].keys() == data.keys() == timestamps.keys()):
            raise EventModelValidationError(
                "These sets of keys must match:\n"
                "event['data'].keys(): {}\n"
                "event['timestamps'].keys(): {}\n"
                "descriptor['data_keys'].keys(): {}\n".format(
                    data.keys(), timestamps.keys(), descriptor["data_keys"].keys()
                )
            )
        if set(filled) - set(data):
            raise EventModelValidationError(
                "Keys in event['filled'] {} must be a subset of those in "
                "event['data'] {}".format(filled.keys(), data.keys())
            )
    event_counter[descriptor["name"]] += len(data)
    return doc


def compose_event(
    *,
    descriptor,
    event_counter,
    data,
    timestamps,
    seq_num=None,
    filled=None,
    uid=None,
    time=None,
    validate=True,
):
    if seq_num is None:
        seq_num = event_counter[descriptor["name"]]
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    if filled is None:
        filled = {}
    doc = {
        "uid": uid,
        "time": time,
        "data": data,
        "timestamps": timestamps,
        "seq_num": seq_num,
        "filled": filled,
        "descriptor": descriptor["uid"],
    }
    if validate:
        schema_validators[DocumentNames.event].validate(doc)
        if not (descriptor["data_keys"].keys() == data.keys() == timestamps.keys()):
            raise EventModelValidationError(
                "These sets of keys must match:\n"
                "event['data'].keys(): {}\n"
                "event['timestamps'].keys(): {}\n"
                "descriptor['data_keys'].keys(): {}\n".format(
                    data.keys(), timestamps.keys(), descriptor["data_keys"].keys()
                )
            )
        if set(filled) - set(data):
            raise EventModelValidationError(
                "Keys in event['filled'] {} must be a subset of those in "
                "event['data'] {}".format(filled.keys(), data.keys())
            )
    event_counter[descriptor["name"]] += 1
    return doc


def compose_descriptor(
    *,
    start,
    streams,
    event_counter,
    name,
    data_keys,
    uid=None,
    time=None,
    object_keys=None,
    configuration=None,
    hints=None,
    validate=True,
):
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    if object_keys is None:
        object_keys = {}
    if configuration is None:
        configuration = {}
    if hints is None:
        hints = {}
    doc = {
        "uid": uid,
        "time": time,
        "run_start": start["uid"],
        "name": name,
        "data_keys": data_keys,
        "object_keys": object_keys,
        "hints": hints,
        "configuration": configuration,
    }
    if validate:
        if name in streams and streams[name] != set(data_keys):
            raise EventModelValidationError(
                "A descriptor with the name {} has already been composed with "
                "data_keys {}. The requested data_keys were {}. All "
                "descriptors in a given stream must have the same "
                "data_keys.".format(name, streams[name], set(data_keys))
            )
        schema_validators[DocumentNames.descriptor].validate(doc)
    if name not in streams:
        streams[name] = set(data_keys)
        event_counter[name] = 1
    return ComposeDescriptorBundle(
        doc,
        partial(compose_event, descriptor=doc, event_counter=event_counter),
        partial(compose_event_page, descriptor=doc, event_counter=event_counter),
    )


def compose_run(*, uid=None, time=None, metadata=None, validate=True):
    """
    Compose a RunStart document and factory functions for related documents.

    Parameters
    ----------
    uid : string, optional
        Unique identifier for this run, conventionally a UUID4. If None is
        given, a UUID4 will be generated.
    time : float, optional
        UNIX epoch time of start of this run. If None is given, the current
        time will be used.
    metadata : dict, optional
        Additional metadata include the document
    validate : boolean, optional
        Validate this document conforms to the schema.

    Returns
    -------
    ComposeRunBundle
    """
    if uid is None:
        uid = str(uuid.uuid4())
    if time is None:
        time = ttime.time()
    if metadata is None:
        metadata = {}
    doc = dict(uid=uid, time=time, **metadata)
    # Define some mutable state to be shared internally by the closures composed
    # below.
    streams = {}
    event_counter = {}
    poison_pill = []
    if validate:
        schema_validators[DocumentNames.start].validate(doc)

    return ComposeRunBundle(
        doc,
        partial(
            compose_descriptor, start=doc, streams=streams, event_counter=event_counter
        ),
        partial(compose_resource, start=doc),
        partial(
            compose_stop,
            start=doc,
            event_counter=event_counter,
            poison_pill=poison_pill,
        ),
    )
