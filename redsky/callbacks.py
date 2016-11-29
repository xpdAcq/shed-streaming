from bluesky.callbacks.core import CallbackBase
from dask.multiprocessing import get
from copy import deepcopy as dc
from uuid import uuid4
import subprocess
import time
import traceback


class MasterDaskCallback(CallbackBase):
    def __init__(self, dask_task_graph, final):
        # throw task graph serialization into database somewhere???
        self.dsk1 = dask_task_graph
        self.final = final

    def __call__(self, name, doc):
        """Dispatch to methods expecting particular doc types."""
        # Replace the placeholders 'name' and 'doc' with the actual name/doc
        temp_dsk = dc(self.dsk1)
        for k, v in temp_dsk.items():
            new = list(v)
            new[1] = name
            if 'doc' in new:
                new[new.index('doc')] = doc
            new_tpl = tuple(new)
            temp_dsk[k] = new_tpl
        return get(temp_dsk, self.final)


class ProcessAndStore(CallbackBase):
    # Subclass from this to make analysis pieces which can add their data to the
    # databroker.
    def __init__(self, analysis_db, run_function, *args, md=None, **kwargs):
        # put in an analysis databroker here
        self.an_db = analysis_db
        self.args = args
        self.kwargs = kwargs
        self.run_function = run_function
        if not md:
            self.md = {}
        self.run_start_uid = None
        self.descriptor_uid = None
        self.seq_num = 0
        self.exit_md = {'exit_status': []}

    def start(self, *docs):

        # TODO: use not hard-coded mds insert
        self.run_start_uid = self.an_db.mds.insert_run_start(
            uid=str(uuid4()), time=time.time(),
            parents=[doc['uid'] for doc in docs],
            function_name=self.run_function.__name__,
            provenance={'args': self.args,
                        'kwargs': self.kwargs,
                        'conda_env': str(subprocess.check_output(
                            ['conda', 'list', '-e']).decode())
                        # FIXME: not conda
                        },
            **self.md)
        return self.an_db.mds.run_start_given_uid(self.run_start_uid)

    def descriptor(self, *docs):
        data_keys = self.run_function.describe(*docs)
        self.descriptor_uid = self.an_db.mds.insert_descriptor(
            run_start=self.run_start_uid,
            data_keys=data_keys,
            time=time.time(),
            uid=str(uuid4()))
        return self.an_db.mds.descriptor_given_uid(self.descriptor_uid)

    def event(self, *docs):
        # If a function higher up in the chain has given us an error, fail
        # and hand that down the pipeline
        # TODO: put this in try?
        for doc in docs:
            if isinstance(doc, Exception):
                return doc
        for doc in docs:
            if not doc['filled']:
                self.an_db.fill_event(doc)  # modifies in place
        try:
            result = self.run_function(*docs, *self.args, **self.kwargs)
            event_uid = self.an_db.mds.insert_event(
                descriptor=self.descriptor_uid,
                uid=str(uuid4()),
                time=time.time(),
                data={k: v for k, v in
                      zip(self.descriptor_uid.keys(), result)},
                timestamps={},
                seq_num=self.seq_num)
            self.seq_num += 1
            result_doc = self.an_db.mds.find_events(uid=event_uid)
            return result_doc
        except Exception as e:
            self.exit_md['exit_status'] = 'failure'
            self.exit_md['reason'] = repr(e)
            self.exit_md['traceback'] = traceback.format_exc()
            return e

    def stop(self, *docs):
        if self.exit_md['exit_status'] is not 'failure':
            self.exit_md['exit_status'] = 'success'
        run_stop_uid = self.an_db.mds.insert_run_stop(
            run_start=self.run_start_uid,
            uid=str(uuid4()),
            time=time.time(), **self.exit_md)
        return self.an_db.mds.run_stop_given_uid(run_stop_uid)
