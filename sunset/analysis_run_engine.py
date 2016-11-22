import time
from uuid import uuid4

import traceback
import subprocess


class AnalysisRunEngine:
    def __init__(self, input_dbs, analysis_db):
        self.input_dbs = input_dbs
        self.an_db = analysis_db

    def __call__(self, hdrs, run_function, *args, md=None,
                 subscription=[], **kwargs):
        if not isinstance(hdrs, list):
            hdrs = [hdrs]
        # issue run start
        run_start_uid = self.an_db.mds.insert_run_start(
            uid=str(uuid4()), time=time.time(),
            parents={'uid': [hdr['start']['uid'] for hdr in hdrs],
                     'db_uids': [hdr['db_uid'] for hdr in hdrs]},
            provenance={'function_name': run_function.__name__,
                        'args': args,
                        'kwargs': kwargs,
                        'conda_env': str(subprocess.check_output(
                            ['conda', 'list', '-e']).decode())})
        # The function fails unless it runs to completion
        exit_md = {'exit_status': 'failure'}

        data_names, data_keys = run_function.describe()
        data_hdr = dict(run_start=run_start_uid,
                        data_keys=data_keys,
                        time=time.time(),
                        uid=str(uuid4()))
        descriptor = self.an_db.mds.insert_descriptor(**data_hdr)
        if not isinstance(subscription, list) and subscription is not None:
            subscription = [subscription]
        event_streams = [
            self.input_dbs[hdr['db_uid']].get_events(hdr, fill=True) for hdr in
            hdrs]
        # run the analysis function
        try:
            rf = run_function(event_streams, *args, fs=self.an_db.fs, **kwargs)
            for i, (res, data) in enumerate(rf):
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
    def __init__(self, function, data_names, data_sub_keys, save_func=None,
                 save_loc=None,
                 spec=None, resource_kwargs={}, datum_kwargs={},
                 save_kwargs={}, save_to_filestore=True):
        self.function = function
        self.data_names = data_names
        self.data_sub_keys = data_sub_keys
        if not hasattr(save_func, '__iter__'):
            save_func = [save_func]
        self.save_func = save_func
        self.save_loc = save_loc
        self.spec = spec
        self.resource_kwargs = resource_kwargs
        self.datum_kwargs = datum_kwargs
        self.save_kwargs = save_kwargs
        self.save_to_filestore = save_to_filestore
        self.__name__ = function.__name__

    def describe(self):
        data_keys = {k: v for k, v in zip(self.data_names, self.data_sub_keys)}
        return self.data_names, data_keys

    def __call__(self, hdrs, *args, fs, **kwargs):
        gen = self.function(*hdrs, *args, **kwargs)
        for output in gen:
            returns = []
            # For each of the outputs save them to filestore, maybe
            for b, s in zip(output, self.save_func):
                if self.save_to_filestore:
                    uid = str(uuid4())
                    # make save name
                    save_name = self.save_loc + uid
                    # Save using the save function
                    s(save_name, b)
                    # Insert into FS
                    uid = str(uuid4())
                    fs_res = fs.insert_resource(self.spec, save_name,
                                                self.resource_kwargs)
                    fs.insert_datum(fs_res, uid, self.datum_kwargs)
                else:
                    if s is None:
                        returns.append(b)
                    else:
                        returns.append(s(b))
            yield returns, output
