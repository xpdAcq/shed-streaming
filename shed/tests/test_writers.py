import bluesky.plans as bp
from bluesky.callbacks import CallbackBase
from rapidz import Stream
from shed.writers import NpyWriter
from databroker._core import _sanitize
import copy
import os


# remove this ASAP
class Retrieve(CallbackBase):
    """Callback for retrieving data from resource and datum documents. This
    can also be used to access file bound data within a scan.

    Parameters
    ----------
    handler_reg: dict
        The handler registry used for looking up the data
    root_map : dict, optional
        Map to replace the root with a new root
    executor : Executor, optional
        If provided run the data loading via the executor, opening the files
        as a future (potentially elsewhere).
    """

    def __init__(self, handler_reg, root_map=None, executor=None):
        self.executor = executor
        if root_map is None:
            root_map = {}
        if handler_reg is None:
            handler_reg = {}
        self.resources = None
        self.handlers = None
        self.datums = None
        self.root_map = root_map
        self.handler_reg = handler_reg

    def __call__(self, name, doc):
        "Dispatch to methods expecting particular doc types."
        ret = getattr(self, name)(doc)
        if ret is None:
            return name, doc
        return name, ret

    def start(self, doc):
        self.resources = {}
        self.handlers = {}
        self.datums = {}

    def resource(self, resource):
        self.resources[resource["uid"]] = resource
        handler = self.handler_reg[resource["spec"]]

        key = (str(resource["uid"]), handler.__name__)

        kwargs = resource["resource_kwargs"]
        rpath = resource["resource_path"]
        root = resource.get("root", "")
        root = self.root_map.get(root, root)
        if root:
            rpath = os.path.join(root, rpath)
        ret = handler(rpath, **kwargs)
        self.handlers[key] = ret

    def datum(self, doc):
        self.datums[doc["datum_id"]] = doc

    def retrieve_datum(self, datum_id):
        doc = self.datums[datum_id]
        resource = self.resources[doc["resource"]]
        handler_class = self.handler_reg[resource["spec"]]
        key = (str(resource["uid"]), handler_class.__name__)
        # If we hand in an executor use it, allowing us to load in parallel
        if self.executor:
            return self.executor.submit(
                self.handlers[key], **doc["datum_kwargs"]
            )
        return self.handlers[key](**doc["datum_kwargs"])

    def fill_event(self, event, fields=True, inplace=True):
        if fields is True:
            fields = set(event["data"])
        elif fields is False:
            # if no fields, we got nothing to do!
            # just return back as-is
            return event
        elif fields:
            fields = set(fields)

        if not inplace:
            ev = _sanitize(event)
            ev = copy.deepcopy(ev)
        else:
            ev = event
        data = ev["data"]
        filled = ev["filled"]
        for k in (
            set(data)
            & fields
            & set(k for k in data if not filled.get(k, True))
        ):
            # Try to fill the data
            try:
                v = self.retrieve_datum(data[k])
                data[k] = v
                filled[k] = True
            # If retrieve fails keep going
            except (ValueError, KeyError):
                pass
        return ev

    def event(self, doc):
        ev = self.fill_event(doc, inplace=False)
        return ev


def test_storage(RE, hw, db, tmpdir):
    source = Stream()
    z = source.Store(str(tmpdir), NpyWriter)
    z.starsink(db.insert)

    L = []
    RE.subscribe(lambda *x: source.emit(x))
    RE.subscribe(lambda *x: L.append(x))
    RE(bp.count([hw.direct_img]))

    rt = Retrieve(handler_reg=db.reg.handler_reg)
    for i, nd in enumerate(db[-1].documents()):
        n2, d2 = rt(*nd)
        print(n2)
        if n2 == 'event':
            print(d2['data']['img'])
            assert d2['data']['img'].shape == (10, 10)


def test_double_storage(RE, hw, db, tmpdir):
    source = Stream()
    z = source.Store(str(tmpdir), NpyWriter)
    z.starsink(db.insert)

    L = []
    RE.subscribe(lambda *x: L.append(x))
    RE(bp.count([hw.direct_img], 2))

    LL = []
    RE.subscribe(lambda *x: LL.append(x))
    RE(bp.count([hw.direct_img], 2))
    for nd1, nd2 in zip(L, LL):
        source.emit(nd1)
        source.emit(nd2)

    rt = Retrieve(handler_reg=db.reg.handler_reg)
    for ii in [-2, -1]:
        for i, nd in enumerate(db[ii].documents()):
            n2, d2 = rt(*nd)
            print(n2)
            if n2 == 'event':
                print(d2['data']['img'])
                assert d2['data']['img'].shape == (10, 10)
