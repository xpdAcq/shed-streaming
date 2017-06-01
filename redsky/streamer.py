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
from copy import deepcopy as dc

import time
from metadatastore.core import doc_or_uid_to_uid
import inspect
import uuid


def db_store_single_resource_single_file(db, fs_data_name_save_map=None):
    """Decorator for adding data to a databroker. This requires all the savers
    to create one resource/datum per file.

    Parameters
    ----------
    db: databroker.Broker instance
        The databroker to store the data in
    fs_data_name_save_map: dict
        The dictionary which maps data names to (Saver, saver_args,
        {saver_kwargs})

    Yields
    -------
    name, doc:
        The name of the document and the document itself
    """
    if fs_data_name_save_map is None:
        fs_data_name_save_map = {}  # {'name': (SaverClass, args, kwargs)}

    def wrap(f):
        def wrapped_f(*args, **kwargs):
            gen = f(*args, **kwargs)
            for name, doc in gen:
                fs_doc = dc(doc)

                if name == 'descriptor':
                    # Mutate the doc here to handle filestore
                    for data_name in fs_data_name_save_map.keys():
                        fs_doc['data_keys'][data_name].update(
                            external='FILESTORE:')

                elif name == 'event':
                    # Mutate the doc here to handle filestore
                    for data_name, save_tuple in fs_data_name_save_map.items():
                        # Create instance of Saver
                        s = save_tuple[0](db.fs, *save_tuple[1],
                                          **save_tuple[2])
                        fs_uid = s.write(fs_doc['data'][data_name])
                        fs_doc['data'][data_name] = fs_uid
                        s.close()

                    doc.update(
                        filled={k: True for k in fs_data_name_save_map.keys()})

                # Always stash the (potentially) filestore mutated doc
                db.mds.insert(name, fs_doc)

                # Always yield the pristine doc
                yield name, doc

        return wrapped_f

    return wrap


def generate_provanance(func):
    d = dict(module=inspect.getmodule(func),
             # this line gets more complex with the integration class
             function_name=func.__name__, )
    return d


class Doc(object):
    def __init__(self, func, output_info=None, input_info=None,
                 stream_keys=None):
        """
        Map a function onto each event in a stream.

        Parameters
        ----------
        func: funtools.partial
            Partial function for data processing
        input_info: dict
            dictionary describing the incoming streams
        output_info: dict
            dictionary describing the resulting stream
        provenance : dict, optional
            metadata about this operation

        Notes
        ------
        input_info = {'input_kwarg': {'name': 'stream_name',
                                  'data_key': 'data_key', }}

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
        self.stream_keys = stream_keys
        if output_info is None:
            output_info = {}
        if input_info is None:
            input_info = {}
        self.run_start_uid = None
        self.input_info = input_info
        self.output_info = output_info
        self.func = func
        self.i = 0
        self.outbound_descriptor_uid = None
        self.new_event = None

    def dispatch(self, nds):
        """Dispatch to methods expecting particular doc types."""
        # If we get multiple streams
        if isinstance(nds[0], tuple):
            names, docs = list(zip(*nds))
            if len(set(names)) > 1:
                raise RuntimeError('Misaligned Streams')
            name = names[0]
        else:
            names, docs = nds
            name = names
            docs = (docs,)
        # if event expose raw event data
        return getattr(self, name)(docs)

    def start(self, docs):
        self.run_start_uid = str(uuid.uuid4())
        new_start_doc = dict(uid=self.run_start_uid,
                             time=time.time(),
                             parents=[doc['uid'] for doc in docs],
                             # parent_keys=[k for k in stream_keys],
                             provenance=generate_provanance(self.func))
        return 'start', new_start_doc

    def descriptor(self, docs):
        if self.run_start_uid is None:
            raise RuntimeError("Received EventDescriptor before "
                               "RunStart.")
        inbound_descriptor_uids = [doc_or_uid_to_uid(doc) for doc in docs]
        self.outbound_descriptor_uid = str(uuid.uuid4())
        new_descriptor = dict(uid=self.outbound_descriptor_uid,
                              time=time.time(),
                              run_start=self.run_start_uid,
                              **self.output_info)
        return 'descriptor', new_descriptor

    def event(self, docs):
        if self.run_start_uid is None:
            raise RuntimeError("Received Event before RunStart.")
        # Make a new event with no data
        self.new_event = dict(uid=str(uuid.uuid4()),
                              time=time.time(),
                              timestamps={},
                              descriptor=self.outbound_descriptor_uid,
                              data={},
                              seq_num=self.i)
        # Update the function kwargs with the event data
        kwargs = {
            stream_key: doc['data'][
                self.input_info[stream_key]['data_key']]
            for stream_key, doc in zip(self.stream_keys, docs)}
        return kwargs

    def generate_event(self, outputs):

        if isinstance(outputs, Exception):
            new_stop = dict(uid=str(uuid.uuid4()),
                            time=time.time(),
                            run_start=self.run_start_uid,
                            reason=repr(outputs),
                            exit_status='failure')
            return 'stop', new_stop

        if len(self.output_info['returns']) == 1:
            outputs = (outputs,)
        # use the return positions list to properly map the
        # output data to the data keys
        for output_name, output in zip(self.output_info['returns'],
                                       outputs):
            self.new_event['data'][output_name] = output
        self.i += 1
        return 'event', self.new_event

    def stop(self, docs):
        if self.run_start_uid is None:
            raise RuntimeError("Received RunStop before RunStart.")
        new_stop = dict(uid=str(uuid.uuid4()),
                        time=time.time(),
                        run_start=self.run_start_uid,
                        exit_status='success')
        self.outbound_descriptor_uid = None
        self.run_start_uid = None
        return 'stop', new_stop


class DocFilter(object):
    def __init__(self, func, output_info=None, input_info=None,
                 stream_keys=None):
        """
        Map a function onto each event in a stream.

        Parameters
        ----------
        func: funtools.partial
            Partial function for data processing
        input_info: dict
            dictionary describing the incoming streams
        output_info: dict
            dictionary describing the resulting stream
        provenance : dict, optional
            metadata about this operation

        Notes
        ------
        input_info = {'input_kwarg': {'name': 'stream_name',
                                  'data_key': 'data_key', }}

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
        self.stream_keys = stream_keys
        if output_info is None:
            output_info = {}
        if input_info is None:
            input_info = {}
        self.run_start_uid = None
        self.input_info = input_info
        self.output_info = output_info
        self.func = func
        self.i = 0
        self.outbound_descriptor_uid = None

    def __call__(self, nds):
        """Dispatch to methods expecting particular doc types."""
        # If we get multiple streams
        if isinstance(nds[0], tuple):
            names, docs = list(zip(*nds))
            if len(set(names)) > 1:
                raise RuntimeError('Misaligned Streams')
            name = names[0]
        else:
            names, docs = nds
            name = names
            docs = (docs,)
        return getattr(self, name)(docs)

    def start(self, docs):
        self.run_start_uid = str(uuid.uuid4())
        new_start_doc = dict(uid=self.run_start_uid,
                             time=time.time(),
                             parents=[doc['uid'] for doc in docs],
                             # parent_keys=[k for k in stream_keys],
                             provenance=generate_provanance(self.func))
        return 'start', new_start_doc

    def descriptor(self, docs):
        if self.run_start_uid is None:
            raise RuntimeError("Received EventDescriptor before "
                               "RunStart.")
        inbound_descriptor_uids = [doc_or_uid_to_uid(doc) for doc in docs]
        self.outbound_descriptor_uid = str(uuid.uuid4())
        new_descriptor = dict(uid=self.outbound_descriptor_uid,
                              time=time.time(),
                              run_start=self.run_start_uid,
                              **self.output_info)
        return 'descriptor', new_descriptor

    def event(self, docs):
        if self.run_start_uid is None:
            raise RuntimeError("Received Event before RunStart.")
        try:
            # Make a new event with no data
            new_event = dict(uid=str(uuid.uuid4()),
                             time=time.time(),
                             timestamps={},
                             descriptor=self.outbound_descriptor_uid,
                             data={},
                             seq_num=self.i)
            # Update the function kwargs with the event data
            kwargs = {
                stream_key: doc['data'][
                    self.input_info[stream_key]['data_key']]
                for stream_key, doc in zip(self.stream_keys, docs)}
            outputs = self.func(**kwargs)

            if len(self.output_info['returns']) == 1:
                outputs = (outputs,)
            # use the return positions list to properly map the
            # output data to the data keys
            for output_name, output in zip(self.output_info['returns'],
                                           outputs):
                new_event['data'][output_name] = output
            self.i += 1
            return 'event', new_event

        except Exception as e:
            new_stop = dict(uid=str(uuid.uuid4()),
                            time=time.time(),
                            run_start=self.run_start_uid,
                            reason=repr(e),
                            exit_status='failure')
            return 'stop', new_stop

    def stop(self, docs):
        if self.run_start_uid is None:
            raise RuntimeError("Received RunStop before RunStart.")
        new_stop = dict(uid=str(uuid.uuid4()),
                        time=time.time(),
                        run_start=self.run_start_uid,
                        exit_status='success')
        self.outbound_descriptor_uid = None
        self.run_start_uid = None
        return 'stop', new_stop
