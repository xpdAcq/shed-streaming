from .analysis_run_engine import RunFunction

"""
Need to give back all the internals of the event except for the uid and
timestamp
"""


class CollectionGen(RunFunction):
    def __init__(self, function):
        super().__init__(function, [None], [None], save_to_filestore=False)

    def __call__(self, hdrs, *args, **kwargs):
        gen = self.function(*hdrs, *args, **kwargs)
        for output in gen:
            if self.data_names == [None]:
                self.data_names = output['descriptor']['data_keys'].keys()
                self.data_sub_keys = output['descriptor']['data_keys'].values()
            yield output['data'].values()
