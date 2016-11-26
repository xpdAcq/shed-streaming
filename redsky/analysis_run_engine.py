import time
from uuid import uuid4

import traceback
import subprocess


class AnalysisRunEngine:
    def __init__(self, analysis_db):
        """Initialize the AnalysisRunEngine

        Parameters
        ----------
        analysis_db: databroker.Broker instance
            The databroker to deposit new headers into
        """
        self.an_db = analysis_db

    def __call__(self, hdrs, run_function, *args, md={},
                 subscription=(), **kwargs):
        """Run the analysis

        Parameters
        ----------
        hdrs: header or list of headers
            The data to be handed to the RunFunction
        run_function: RunFunction instance
            The RunFunction to process the data
        args: args, optional
            Arguments to be passed to the RunFunction call
        md: dict, optional
            Metadata to be added to the analysis header
        subscription: iterable of functions, optional
            Call back function to run on the processed data
        kwargs: kwargs, optional
            Key word arguments passed to the RunFunction call

        Returns
        -------

        """
        if not isinstance(hdrs, list):
            hdrs = [hdrs]
        # issue run start
        run_start_uid = self.an_db.mds.insert_run_start(
            uid=str(uuid4()), time=time.time(),
            parents=[hdr['start']['uid'] for hdr in hdrs],
            function_name=run_function.__name__,
            provenance={'args': args,
                        'kwargs': kwargs,
                        'conda_env': str(subprocess.check_output(
                            ['conda', 'list', '-e']).decode())},
            **md)

        # The function fails unless it runs to completion
        exit_md = {'exit_status': 'failure'}

        descriptor = None
        if not hasattr(subscription, '__iter__') and subscription is not None:
            subscription = [subscription]
        fill = run_function.fill
        if len(fill) != len(hdrs) and len(fill) == 1:
            fill *= len(hdrs)
        event_streams = [self.an_db.get_events(hdr, fill=f) for hdr, f in zip(
            hdrs, fill)]
        # run the analysis function
        try:
            rf = run_function(event_streams, *args, fs=self.an_db.fs, **kwargs)
            for i, (res, data) in enumerate(rf):
                if descriptor is None:
                    data_names, data_keys = run_function.describe()
                    data_hdr = dict(run_start=run_start_uid,
                                    data_keys=data_keys,
                                    time=time.time(),
                                    uid=str(uuid4()))
                    descriptor = self.an_db.mds.insert_descriptor(**data_hdr)
                self.an_db.mds.insert_event(
                    descriptor=descriptor,
                    uid=str(uuid4()),
                    time=time.time(),
                    data={k: v for k, v in zip(data_names, res)},
                    timestamps={},
                    seq_num=i)
                for subs in subscription:
                    subs(data)
            exit_md['exit_status'] = 'success'
        except Exception as e:
            print(e)
            # Just for testing
            print(traceback.format_exc())

            # Analysis failed!
            exit_md['exit_status'] = 'failure'
            exit_md['reason'] = repr(e)
            exit_md['traceback'] = traceback.format_exc()
        finally:
            self.an_db.mds.insert_run_stop(run_start=run_start_uid,
                                           uid=str(uuid4()),
                                           time=time.time(), **exit_md)
            return run_start_uid


class RunFunction:
    def __init__(self, function, data_names, descriptors, save_func=None,
                 save_loc=None, ext=None,
                 spec=None, resource_kwargs={}, datum_kwargs={},
                 save_kwargs={}, save_to_filestore=True, fill=True):
        """Initialize a RunFunction

        Parameters
        ----------
        function: generator
            The generator which takes in event streams and processes them
        data_names: list of str
            The names for each piece data that comes out of the function
        descriptors: list of dicts
            The data descriptor
        save_func: function or None, optional
            Function to save the resulting data to disk. If None data is not
            saved to disk.
        save_loc: str, optional
            Path to the directory where the files are to be saved
        spec: list of str or str, optional
            Filestore spec, defaults to None
        resource_kwargs: dict, optional
            Keyword arguments passed to insert_resource, defaults to {}
        datum_kwargs: dict, optional
            Keyword arguments passed to insert_datum, defaults to {}
        save_kwargs: dict, optional
            Keyword arguments passed to the save function
        save_to_filestore: bool
            The
        """
        # TODO: Need to store function location and other things needed to
        # rehydrate it.
        self.ext = ext
        self.function = function
        self.data_names = data_names
        self.data_sub_keys = descriptors
        if not hasattr(save_func, '__iter__'):
            save_func = [save_func] * len(data_names)
        self.save_func = save_func
        self.save_loc = save_loc
        self.spec = spec
        self.resource_kwargs = resource_kwargs
        self.datum_kwargs = datum_kwargs
        self.save_kwargs = save_kwargs
        self.save_to_filestore = save_to_filestore
        self.__name__ = function.__name__
        self.fill = fill

    def describe(self):
        data_keys = {k: v for k, v in zip(self.data_names, self.data_sub_keys)}
        return self.data_names, data_keys

    def __call__(self, hdrs, *args, fs, **kwargs):
        gen = self.function(*hdrs, *args, **kwargs)
        for output in gen:
            returns = []
            # For each of the outputs save them to filestore, maybe
            if not isinstance(output, (tuple, list)):
                op = [output]
            else:
                op = output
            for b, s in zip(op, self.save_func):
                if self.save_to_filestore:
                    uid = str(uuid4())
                    # make save name
                    save_name = self.save_loc + uid + self.ext
                    # Save using the save function
                    s(save_name, b)
                    # Insert into FS
                    uid = str(uuid4())
                    fs_res = fs.insert_resource(self.spec, save_name,
                                                self.resource_kwargs)
                    fs.insert_datum(fs_res, uid, self.datum_kwargs)
                    returns.append(uid)
                elif s:
                    returns.append(s(b))
                else:
                    returns = list(op)
            yield returns, output
