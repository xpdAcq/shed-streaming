from ..streams import Stream
from ..streamer import StoreSink
from numpy.testing import assert_allclose
import numpy as np
import os
import uuid
from functools import partial


class NpyWriter:
    """
    Each call to the ``write`` method saves a file and creates a new filestore
    resource and datum record.
    """

    SPEC = 'npy'

    def __init__(self, fs, root):
        self._root = root
        self._closed = False
        self._fs = fs
        # Open and stash a file handle (e.g., h5py.File) if applicable.

    def write(self, data):
        """
        Save data to file, generate and insert new resource and datum.
        """
        if self._closed:
            raise RuntimeError('This writer has been closed.')
        fp = os.path.join(self._root, '{}.npy'.format(str(uuid.uuid4())))
        np.save(fp, data)
        resource = self._fs.insert_resource(self.SPEC, fp, resource_kwargs={})
        datum_id = str(uuid.uuid4())
        self._fs.insert_datum(resource=resource, datum_id=datum_id,
                              datum_kwargs={})
        return datum_id

    def close(self):
        self._closed = True

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def test_map(exp_db, start_uid1):
    source = Stream()

    def add5(img):
        return img + 5

    L = source.dstarmap(add5,
                        input_info=[('img', 'pe1_image')],
                        output_info=[('img',
                                      {'dtype': 'array',
                                       'source': 'testing'})]).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'], s[1]['data']['pe1_image'] + 5)


def test_double_map(exp_db, start_uid1):
    source = Stream()
    source2 = Stream()

    def add_imgs(img1, img2):
        return img1 + img2

    L = source.zip(source2).dstarmap(
        add_imgs,
        input_info=[('img1', 'pe1_image'), ('img2', 'pe1_image')],
        output_info=[
            ('img',
             {'dtype': 'array',
              'source': 'testing'})]).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
        source2.emit(a)
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'],
                            add_imgs(s[1]['data']['pe1_image'],
                                     s[1]['data']['pe1_image']))
        if l[0] == 'stop':
            assert l[1]['exit_status'] == 'success'


def test_filter(exp_db, start_uid1):
    source = Stream()

    def f(img1):
        return isinstance(img1, np.ndarray)

    L = source.filter(f).sink_to_list()
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1, fill=True)
    for a in s:
        source.emit(a)
    for l, s in zip(L, exp_db.restream(ih1, fill=True)):
        if l[0] == 'event':
            assert_allclose(l[1]['data']['img'], s[1]['data']['pe1_image'])
        if l[0] == 'stop':
            print(l)
            assert l[1]['exit_status'] == 'success'


def test_combine_latest(exp_db, start_uid1, start_uid3):
    source = Stream()
    source2 = Stream()

    L = source.combine_latest(source2).sink_to_list()
    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s = exp_db.restream(ih1, fill=True)
    s2 = exp_db.restream(ih2, fill=True)

    def zip_emiter(sources, streams):
        status = [True] * len(sources)
        while all(status):
            for i, (s, stream) in enumerate(zip(sources, streams)):
                if status[i]:
                    try:
                        stream.emit(next(s))
                    except StopIteration:
                        status[i] = False

    zip_emiter((s, s2), (source, source2))
    for l in L:
        print(list(zip(*l))[0])


def test_zip(exp_db, start_uid1, start_uid3):
    source = Stream()
    source2 = Stream()

    L = source.zip(source2).sink_to_list()
    ih1 = exp_db[start_uid1]
    ih2 = exp_db[start_uid3]
    s = exp_db.restream(ih1, fill=True)
    s2 = exp_db.restream(ih2, fill=True)
    for b in s2:
        print(b[0])
        source2.emit(b)
    for a in s:
        print(a[0])
        source.emit(a)
    for l1, l2 in L:
        print(l1, l2)
        assert l1 != l2


def test_workflow(exp_db, start_uid1, tmp_dir):
    def subs(x1, x2):
        return x1 - x2

    hdr = exp_db[start_uid1]

    raw_data = hdr.stream(fill=True)
    dark_data = exp_db[hdr['start']['sc_dk_field_uid']].stream(fill=True)
    rds = Stream()
    dark_data_stream = Stream()

    store_sink = StoreSink(db=exp_db,
                           external_writers={'image': partial(NpyWriter,
                                                              root=tmp_dir)})

    img_stream = rds.zip(dark_data_stream
                         ).dstarmap(subs,
                                    input_info=[
                                        ('x1', 'pe1_image'),
                                        ('x2', 'pe1_image')],
                                    output_info=[('image', {
                                        'dtype': 'array',
                                        'source': 'testing'})]
                                    )
    img_stream.sink(store_sink)
    L = img_stream.sink_to_list()

    for d in dark_data:
        dark_data_stream.emit(d)
    for d in raw_data:
        rds.emit(d)
    for l in L:
        print(l)
    print(exp_db[-1])
