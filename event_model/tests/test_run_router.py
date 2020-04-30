from collections import defaultdict

from event_model import compose_run, RunRouter


def test_subfactory():
    factory_documents = defaultdict(list)
    subfactory_documents = defaultdict(list)

    def factory(name, start_doc):
        def collect_factory_documents(name, doc):
            factory_documents[name].append(doc)

        def collect_subfactory_documents(name, doc):
            subfactory_documents[name].append(doc)

        def subfactory(name, descriptor_doc):
            return [collect_factory_documents]

        return [collect_factory_documents], [subfactory]

    rr = RunRouter([factory])

    run_bundle = compose_run()
    rr("start", run_bundle.start_doc)
    assert len(factory_documents) == 1
    assert len(factory_documents["start"]) == 1
    assert factory_documents["start"] == [run_bundle.start_doc]
    assert len(subfactory_documents) == 0

    desc_bundle = run_bundle.compose_descriptor(
        data_keys={"motor": {"shape": [], "dtype": "number", "source": "..."}},
        name="primary",
    )
    rr("descriptor", desc_bundle.descriptor_doc)
    assert len(factory_documents) == 2
    assert len(factory_documents["start"]) == 1
    assert factory_documents["start"] == [run_bundle.start_doc]
    assert factory_documents["descriptor"] == [desc_bundle.descriptor_doc]
    assert len(subfactory_documents) == 2
    assert subfactory_documents["start"] == [desc_bundle.descriptor_doc]
    assert subfactory_documents["descriptor"] == [desc_bundle.descriptor_doc]
