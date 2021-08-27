"""Helper functions to pack/unpack/rechuck event and datum pages."""

import itertools
from collections import defaultdict


def unpack_event_page(event_page):
    """
    Transform an EventPage document into individual Event documents.

    Parameters
    ----------
    event_page : dict

    Yields
    ------
    event : dict
    """
    descriptor = event_page["descriptor"]
    data_list = _transpose_dict_of_lists(event_page["data"])
    timestamps_list = _transpose_dict_of_lists(event_page["timestamps"])
    filled_list = _transpose_dict_of_lists(event_page.get("filled", {}))
    for uid, time, seq_num, data, timestamps, filled in itertools.zip_longest(
        event_page["uid"],
        event_page["time"],
        event_page["seq_num"],
        data_list,
        timestamps_list,
        filled_list,
        fillvalue={},
    ):
        event = {
            "descriptor": descriptor,
            "uid": uid,
            "time": time,
            "seq_num": seq_num,
            "data": data,
            "timestamps": timestamps,
            "filled": filled,
        }
        yield event


def pack_datum_page(*datum):
    """
    Transform one or more Datum documents into a DatumPage document.

    Parameters
    ----------
    *datum : dicts
        any number of Datum documents

    Returns
    -------
    datum_page : dict
    """
    if not datum:
        raise ValueError(
            "The pack_datum_page() function was called with empty *args. "
            "Cannot create an DatumPage from an empty collection of Datum "
            "because the 'resource' field in a DatumPage cannot be NULL."
        )
    datum_id_list = []
    datum_kwarg_list = []
    for datum_ in datum:
        datum_id_list.append(datum_["datum_id"])
        datum_kwarg_list.append(datum_["datum_kwargs"])
    datum_page = {
        "resource": datum_["resource"],
        "datum_id": datum_id_list,
        "datum_kwargs": _transpose_list_of_dicts(datum_kwarg_list),
    }
    return datum_page


def unpack_datum_page(datum_page):
    """
    Transform a DatumPage document into individual Datum documents.

    Parameters
    ----------
    datum_page : dict

    Yields
    ------
    datum : dict
    """
    resource = datum_page["resource"]
    datum_kwarg_list = _transpose_dict_of_lists(datum_page["datum_kwargs"])
    for datum_id, datum_kwargs in itertools.zip_longest(
        datum_page["datum_id"], datum_kwarg_list, fillvalue={}
    ):
        datum = {
            "datum_id": datum_id,
            "datum_kwargs": datum_kwargs,
            "resource": resource,
        }
        yield datum


def rechunk_event_pages(event_pages, chunk_size):
    """
    Resizes the event_pages in a iterable of event_pages.

    Parameters
    ----------
    event_pages: Iterabile
        An iterable of event_pages
    chunk_size: integer
        Size of pages to yield

    Yields
    ------
    event_page : dict
    """
    remainder = chunk_size
    chunk_list = []

    def page_chunks(page, chunk_size, remainder):
        """
        Yields chunks of a event_page.
        The first chunk will be of size remainder, the following chunks will be
        of size chunk_size. The last chunk will be what ever is left over.
        """
        array_keys = ["seq_num", "time", "uid"]
        page_size = len(page["uid"])  # Number of events in the page.

        # Make a list of the chunk indexes.
        chunks = [(0, remainder)]
        chunks.extend(
            [(i, i + chunk_size) for i in range(remainder, page_size, chunk_size)]
        )

        for start, stop in chunks:
            yield {
                "descriptor": page["descriptor"],
                **{key: page[key][start:stop] for key in array_keys},
                "data": {
                    key: page["data"][key][start:stop] for key in page["data"].keys()
                },
                "timestamps": {
                    key: page["timestamps"][key][start:stop]
                    for key in page["timestamps"].keys()
                },
                "filled": {
                    key: page["filled"][key][start:stop] for key in page["data"].keys()
                },
            }

    for page in event_pages:
        new_chunks = page_chunks(page, chunk_size, remainder)
        for chunk in new_chunks:
            remainder -= len(chunk["uid"])  # Subtract the size of the chunk.
            chunk_list.append(chunk)
            if remainder == 0:
                yield merge_event_pages(chunk_list)
                remainder = chunk_size
                chunk_list = []
    if chunk_list:
        yield merge_event_pages(chunk_list)


def merge_event_pages(event_pages):
    """
    Combines a iterable of event_pages to a single event_page.

    Parameters
    ----------
    event_pages: Iterabile
        An iterable of event_pages

    Returns
    ------
    event_page : dict
    """
    pages = list(event_pages)
    if len(pages) == 1:
        return pages[0]

    array_keys = ["seq_num", "time", "uid"]

    return {
        "descriptor": pages[0]["descriptor"],
        **{
            key: list(itertools.chain.from_iterable([page[key] for page in pages]))
            for key in array_keys
        },
        "data": {
            key: list(
                itertools.chain.from_iterable([page["data"][key] for page in pages])
            )
            for key in pages[0]["data"].keys()
        },
        "timestamps": {
            key: list(
                itertools.chain.from_iterable(
                    [page["timestamps"][key] for page in pages]
                )
            )
            for key in pages[0]["data"].keys()
        },
        "filled": {
            key: list(
                itertools.chain.from_iterable([page["filled"][key] for page in pages])
            )
            for key in pages[0]["data"].keys()
        },
    }


def rechunk_datum_pages(datum_pages, chunk_size):
    """
    Resizes the datum_pages in a iterable of event_pages.

    Parameters
    ----------
    datum_pages: Iterabile
        An iterable of datum_pages
    chunk_size: integer
        Size of pages to yield

    Yields
    ------
    datum_page : dict
    """
    remainder = chunk_size
    chunk_list = []

    def page_chunks(page, chunk_size, remainder):
        """
        Yields chunks of a datum_page.
        The first chunk will be of size remainder, the following chunks will be
        of size chunk_size. The last chunk will be what ever is left over.
        """

        array_keys = ["datum_id"]
        page_size = len(page["datum_id"])  # Number of datum in the page.

        # Make a list of the chunk indexes.
        chunks = [(0, remainder)]
        chunks.extend(
            [(i, i + chunk_size) for i in range(remainder, page_size, chunk_size)]
        )

        for start, stop in chunks:
            yield {
                "resource": page["resource"],
                **{key: page[key][start:stop] for key in array_keys},
                "datum_kwargs": {
                    key: page["datum_kwargs"][key][start:stop]
                    for key in page["datum_kwargs"].keys()
                },
            }

    for page in datum_pages:
        new_chunks = page_chunks(page, chunk_size, remainder)
        for chunk in new_chunks:
            remainder -= len(chunk["datum_id"])  # Subtract the size of the chunk.
            chunk_list.append(chunk)
            if remainder == 0:
                yield merge_datum_pages(chunk_list)
                remainder = chunk_size
                chunk_list = []
    if chunk_list:
        yield merge_datum_pages(chunk_list)


def merge_datum_pages(datum_pages):
    """
    Combines a iterable of datum_pages to a single datum_page.

    Parameters
    ----------
    datum_pages: Iterabile
        An iterable of datum_pages

    Returns
    ------
    datum_page : dict
    """
    pages = list(datum_pages)
    if len(pages) == 1:
        return pages[0]

    array_keys = ["datum_id"]

    return {
        "resource": pages[0]["resource"],
        **{
            key: list(itertools.chain.from_iterable([page[key] for page in pages]))
            for key in array_keys
        },
        "datum_kwargs": {
            key: list(
                itertools.chain.from_iterable(
                    [page["datum_kwargs"][key] for page in pages]
                )
            )
            for key in pages[0]["datum_kwargs"].keys()
        },
    }


def bulk_events_to_event_pages(bulk_events):
    """
    Transform a BulkEvents document into a list of EventPage documents.

    Note: The BulkEvents layout has been deprecated in favor of EventPage.

    Parameters
    ----------
    bulk_events : dict

    Returns
    -------
    event_pages : list
    """
    # This is for a deprecated document type, so we are not being fussy
    # about efficiency/laziness here.
    event_pages = {}  # descriptor uid mapped to page
    for events in bulk_events.values():
        for event in events:
            descriptor = event["descriptor"]
            try:
                page = event_pages[descriptor]
            except KeyError:
                page = {"time": [], "uid": [], "seq_num": [], "descriptor": descriptor}
                page["data"] = {k: [] for k in event["data"]}
                page["timestamps"] = {k: [] for k in event["timestamps"]}
                page["filled"] = {k: [] for k in event.get("filled", {})}
                event_pages[descriptor] = page
            page["uid"].append(event["uid"])
            page["time"].append(event["time"])
            page["seq_num"].append(event["seq_num"])
            page_data = page["data"]
            for k, v in event["data"].items():
                page_data[k].append(v)
            page_timestamps = page["timestamps"]
            for k, v in event["timestamps"].items():
                page_timestamps[k].append(v)
            page_filled = page["filled"]
            for k, v in event.get("filled", {}).items():
                page_filled[k].append(v)
    return list(event_pages.values())


def bulk_datum_to_datum_page(bulk_datum):
    """
    Transform one BulkDatum into one DatumPage.

    Note: There is only one known usage of BulkDatum "in the wild", and the
    BulkDatum layout has been deprecated in favor of DatumPage.
    """
    datum_page = {
        "datum_id": bulk_datum["datum_ids"],
        "resource": bulk_datum["resource"],
        "datum_kwargs": _transpose_list_of_dicts(bulk_datum["datum_kwarg_list"]),
    }
    return datum_page


def _transpose_list_of_dicts(list_of_dicts):
    "Transform list-of-dicts into dict-of-lists (i.e. DataFrame-like)."
    dict_of_lists = defaultdict(list)
    for row in list_of_dicts:
        for k, v in row.items():
            dict_of_lists[k].append(v)
    return dict(dict_of_lists)


def _transpose_dict_of_lists(dict_of_lists):
    "Transform dict-of-lists (i.e. DataFrame-like) into list-of-dicts."
    list_of_dicts = []
    keys = list(dict_of_lists)
    for row in zip(*(dict_of_lists[k] for k in keys)):
        list_of_dicts.append(dict(zip(keys, row)))
    return list_of_dicts


def pack_event_page(*events):
    """
    Transform one or more Event documents into an EventPage document.

    Parameters
    ----------
    *event : dicts
        any number of Event documents

    Returns
    -------
    event_page : dict
    """
    if not events:
        raise ValueError(
            "The pack_event_page() function was called with empty *args. "
            "Cannot create an EventPage from an empty collection of Events "
            "because the 'descriptor' field in an EventPage cannot be NULL."
        )
    time_list = []
    uid_list = []
    seq_num_list = []
    data_list = []
    filled_list = []
    timestamps_list = []
    for event in events:
        time_list.append(event["time"])
        uid_list.append(event["uid"])
        seq_num_list.append(event["seq_num"])
        filled_list.append(event.get("filled", {}))
        data_list.append(event["data"])
        timestamps_list.append(event["timestamps"])
    event_page = {
        "time": time_list,
        "uid": uid_list,
        "seq_num": seq_num_list,
        "descriptor": event["descriptor"],
        "filled": _transpose_list_of_dicts(filled_list),
        "data": _transpose_list_of_dicts(data_list),
        "timestamps": _transpose_list_of_dicts(timestamps_list),
    }
    return event_page
