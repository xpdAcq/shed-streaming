"""Module for automatically storing processed data in a databroker"""
##############################################################################
#
# redsky            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Christopher J. Wright
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
import time
import uuid
import six
from itertools import tee


def doc_or_uid_to_uid(doc_or_uid):
    """Given Document or uid return the uid

    Parameters
    ----------
    doc_or_uid : dict or str
        If str, then assume uid and pass through, if not, return
        the 'uid' field

    Returns
    -------
    uid : str
        A string version of the uid of the given document

    """
    if not isinstance(doc_or_uid, six.string_types):
        doc_or_uid = doc_or_uid['uid']
    return doc_or_uid


def store_dec(db, external_writers=None):
    """Decorate a generator of documents to save them to the databases.

    The input stream of (name, document) pairs passes through unchanged.
    As a side effect, documents are inserted into the databases and external
    files may be written.

    Parameters
    ----------
    db: ``databroker.Broker`` instance
        The databroker to store the documents in, must have writeable
        metadatastore and writeable filestore if ``external`` is not empty.
    external_writers : dict
        Maps data keys to a ``WriterClass``, which is responsible for writing
        data to disk and creating a record in filestore. It will be
        instantiated (possible multiple times) with the argument ``db.fs``.
        If it requires additional arguments, use ``functools.partial`` to
        produce a callable that requires only ``db.fs`` to instantiate the
        ``WriterClass``.
        """
    if external_writers is None:
        external_writers = {}  # {'name': WriterClass}

    def wrap(f):
        def wrapped_f(*args, **kwargs):
            gen = f(*args, **kwargs)
            for name, doc in gen:
                # doc will pass through unchanged; fs_doc may be modified to
                # replace some values with references to filestore.
                fs_doc = dict(doc)

                if name == 'start':
                    # Make a fresh instance of any WriterClass classes.
                    writers = {data_key: cl(db.fs)
                               for data_key, cl in external_writers.items()}

                if name == 'descriptor':
                    # Mutate fs_doc here to mark data as external.
                    for data_name in external_writers.keys():
                        # data doesn't have to exist
                        if data_name in fs_doc['data_keys']:
                            fs_doc['data_keys'][data_name].update(
                                external='FILESTORE:')

                elif name == 'event':
                    # We need a selectively deeper copy since we will mutate
                    # fs_doc['data'].
                    fs_doc['data'] = dict(fs_doc['data'])
                    # The writer writes data to an external file, creates a
                    # datum record in the filestore database, and return that
                    # datum_id. We modify fs_doc in place, replacing the data
                    # values with that datum_id.
                    for data_key, writer in writers.items():
                        # data doesn't have to exist
                        if data_key in fs_doc['data']:
                            fs_uid = writer.write(fs_doc['data'][data_key])
                            fs_doc['data'][data_key] = fs_uid

                    doc.update(
                        filled={k: False for k in external_writers.keys()})

                elif name == 'stop':
                    for data_key, writer in list(writers.items()):
                        writer.close()
                        writers.pop(data_key)

                # The mutated fs_doc is inserted into metadatastore.
                fs_doc.pop('filled', None)
                fs_doc.pop('_name', None)
                db.mds.insert(name, fs_doc)

                # The pristine doc is yielded.
                yield name, doc

        return wrapped_f

    return wrap


def event_map(input_info, output_info, provenance=None):
    """
    Map a function onto each event in a stream.

    Parameters
    ----------
    input_info: dict
        dictionary describing the incoming streams
    output_info: dict
        dictionary describing the resulting stream
    provenance : dict, optional
        metadata about this operation

    Notes
    ------
    input_info = {'input_kwarg': {'name': 'stream_name',
                              'data_key': 'data_key',
                              'remux': ('remux_function',
                                        'input_kwarg_of_remuxed_to')}}

    Examples
    ---------
    >>> @event_map({'img': {'name': 'primary', 'data_key': 'pe1_image'}},
    >>> {'data_keys': {'img': {'dtype': 'array'}},
    >>>            'name': 'primary',
    >>>            'returns': ['img'],
    >>>            })
    >>> def multiply_by_two(img):
    >>>     return img * 2
    >>> output_stream = multiply_by_two(db[-1].restream(fill=True))
    """
    if provenance is None:
        provenance = {}
    if set(output_info['data_keys']) != set(output_info['returns']):
        raise RuntimeError("The `data_keys` did not match the `returns")

    def outer(f):
        def inner(**kwargs):
            if set(input_info.keys()).intersection(set(kwargs.keys())) != set(
                    input_info.keys()):
                raise RuntimeError(
                    "The input data keys were not in the function inputs")
            # If any stream needs cleaning/remuxing do it here
            # We'll capture the relevant info into provenance
            # Note this remux is not saved as a bundle
            streams = {k: kwargs[k] for k in input_info.keys()}
            for k in input_info.keys():
                if input_info[k].get('remux', None):
                    remux_func, base_stream = input_info[k]['remux']
                    remux_streams = list(tee(kwargs[base_stream], 2))
                    streams[k] = remux_func(streams[k], remux_streams.pop())
                    streams[base_stream] = remux_streams.pop()

            # Need a reproducible handle in the generators
            stream_keys = streams.keys()
            stream_values = [streams[k] for k in stream_keys]

            # Initialize counter and uids
            run_start_uid = None
            inbound_descriptor_uids = None
            outbound_descriptor_uid = None
            i = 0

            for name_doc_pairs in zip(*stream_values):
                # Useful lists
                names, docs = zip(*name_doc_pairs)
                print(names)

                if all([name == 'start' for name in names]):
                    run_start_uid = str(uuid.uuid4())
                    new_start_doc = dict(uid=run_start_uid,
                                         time=time.time(),
                                         parents=[doc['uid'] for doc in docs],
                                         parent_keys=[k for k in stream_keys],
                                         provenance=provenance)
                    yield 'start', new_start_doc

                elif (all([name == 'descriptor' for name in names]) and
                      all([doc.get('name') == input_info[k]['name']
                           for k, doc in zip(stream_keys, docs)])):
                    if run_start_uid is None:
                        raise RuntimeError("Received EventDescriptor before "
                                           "RunStart.")

                    inbound_descriptor_uids = [doc_or_uid_to_uid(doc) for
                                               doc in docs]
                    outbound_descriptor_uid = str(uuid.uuid4())
                    new_descriptor = dict(uid=outbound_descriptor_uid,
                                          time=time.time(),
                                          run_start=run_start_uid,
                                          **output_info)
                    yield 'descriptor', new_descriptor

                elif (all([name == 'event' for name in names]) and
                      all([doc_or_uid_to_uid(doc['descriptor']) ==
                           descriptor_uid for doc, descriptor_uid in
                           zip(docs, inbound_descriptor_uids)])):
                    if run_start_uid is None:
                        raise RuntimeError("Received Event before RunStart.")
                    try:
                        # Make a new event with no data
                        new_event = dict(uid=str(uuid.uuid4()),
                                         time=time.time(),
                                         timestamps={},
                                         descriptor=outbound_descriptor_uid,
                                         data={},
                                         seq_num=i)
                        # Update the function kwargs with the event data
                        kwargs_update = {
                            stream_key: doc['data'][input_info[stream_key][
                                'data_key']]
                            for stream_key, doc in
                            zip(stream_keys, docs)}
                        kwargs.update(kwargs_update)
                        outputs = f(**kwargs)

                        if len(output_info['returns']) == 1:
                            outputs = (outputs,)
                        # use the return positions list to properly map the
                        # output data to the data keys
                        for output_name, output in zip(
                                output_info['returns'], outputs):
                            new_event['data'][output_name] = output
                        i += 1

                        yield 'event', new_event
                    except Exception as e:
                        new_stop = dict(uid=str(uuid.uuid4()),
                                        time=time.time(),
                                        run_start=run_start_uid,
                                        reason=repr(e),
                                        exit_status='failure')
                        yield 'stop', new_stop
                        raise

                elif all([name == 'stop' for name in names]):
                    if run_start_uid is None:
                        raise RuntimeError("Received RunStop before RunStart.")
                    new_stop = dict(uid=str(uuid.uuid4()),
                                    time=time.time(),
                                    run_start=run_start_uid,
                                    exit_status='success')
                    outbound_descriptor_uid = None
                    run_start_uid = None
                    yield 'stop', new_stop

        return inner

    return outer
