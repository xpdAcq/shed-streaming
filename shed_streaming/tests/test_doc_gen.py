from shed_streaming.doc_gen import CreateDocs


def test_docs():
    cd = CreateDocs(("ct",), my_md="md")

    start = cd.start_doc(None)
    for k in ["uid", "time", "scan_id"]:
        assert k in start
    assert start["my_md"] == "md"
    desc = cd.descriptor(1)
    for k in ["uid", "time", "run_start", "name", "data_keys"]:
        assert k in desc
    assert desc["data_keys"]["ct"]["dtype"] == "number"
    assert desc["data_keys"]["ct"]["shape"] == []
    ev = cd.event(1)
    for k in [
        "uid",
        "time",
        "descriptor",
        "filled",
        "data",
        "seq_num",
        "timestamps",
    ]:
        assert k in ev
    assert ev["data"]["ct"] == 1
    stop = cd.stop(None)
    for k in ["uid", "time", "run_start", "exit_status"]:
        assert k in stop


def test_create_docs():
    cd = CreateDocs(("ct",), my_md="md")

    n, start = cd.create_doc("start", None)
    for k in ["uid", "time"]:
        assert k in start
    assert start["my_md"] == "md"
    n, desc = cd.create_doc("descriptor", 1)
    for k in ["uid", "time", "run_start", "name", "data_keys"]:
        assert k in desc
    assert desc["data_keys"]["ct"]["dtype"] == "number"
    assert desc["data_keys"]["ct"]["shape"] == []
    n, ev = cd.create_doc("event", 1)
    for k in [
        "uid",
        "time",
        "descriptor",
        "filled",
        "data",
        "seq_num",
        "timestamps",
    ]:
        assert k in ev
    assert ev["data"]["ct"] == 1
    n, stop = cd.create_doc("stop", None)
    for k in ["uid", "time", "run_start", "exit_status"]:
        assert k in stop
