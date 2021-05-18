import event_model
import itertools


def scrub(hdrs, new_epoch, start_scan_id, filter_start=lambda x: x):
    """
    Scrubs a set of headers of most identifying information.

    Leaves the internal time and scan_id differences between the headers intact.

    Parameters
    ----------
    hdrs : Iterable[BlueskyRun]
        The headers to sanitize

    new_epoch : float
        The first header in hdrs is deemed to be at this time, all other headers
        will have their times adjusted so that the relative time is the same.

    start_scan_id : int

        The first header is deemed to be at this scan_id, the rest of the
        scan_ids will be adjusted accordingly.

    filter_start : Callable[Dict][Dict], optional
        Return a the filtered start document


    Yields
    ------
    name : str
    doc : dict

    """
    time_offset = None
    scan_id_offset = None

    for h, first in zip(hdrs, itertools.chain([True], itertools.repeat(False))):
        if first:
            time_offset = h.metadata["start"]["time"] - new_epoch
            scan_id_offset = h.metadata["start"]["scan_id"] - start_scan_id

        docs = h.documents(fill="no")

        _, start = next(docs)
        start = dict(start)
        start = filter_start(start)
        start.pop("uid")

        new_time = start.pop("time") - time_offset
        new_scan_id = start.pop("scan_id", 0) - scan_id_offset

        desc_map = {}
        run_bundle = event_model.compose_run(
            time=new_time, metadata={**start, "scan_id": new_scan_id}
        )
        yield "start", run_bundle.start_doc

        for name, doc in docs:
            if name == "datum_page":
                yield name, doc
            elif name == "resource":
                res = dict(doc)
                res["run_uid"] = run_bundle.start_doc["uid"]
            elif name == "descriptor":
                desc = dict(doc)
                desc["configuration"] = dict(desc["configuration"])
                for k, v in desc["configuration"].items():
                    # try not to mutate the input!
                    desc["configuration"][k] = dict(v)
                    desc["configuration"][k]["timestamps"] = {
                        k: t - time_offset for k, t in v["timestamps"].items()
                    }
                desc_bundle = desc_map[desc["uid"]] = run_bundle.compose_descriptor(
                    name=desc["name"],
                    time=desc["time"] - time_offset,
                    data_keys=desc["data_keys"],
                    configuration=desc["configuration"],
                    object_keys=desc["object_keys"],
                )
                yield "descriptor", desc_bundle.descriptor_doc
            elif name == "event_page":
                desc_bundle = desc_map[doc["descriptor"]]
                event_page = desc_bundle.compose_event_page(
                    seq_num=doc["seq_num"],
                    data=doc["data"],
                    time=[t - time_offset for t in doc["time"]],
                    timestamps={
                        k: [t - time_offset for t in v]
                        for k, v in doc["timestamps"].items()
                    },
                )
                yield "event_page", event_page
            elif name == "stop":
                yield name, run_bundle.compose_stop(
                    exit_status=doc["exit_status"],
                    time=doc["time"] - time_offset,
                    reason=doc["reason"],
                )
            else:
                raise Exception("unexpected document!")
