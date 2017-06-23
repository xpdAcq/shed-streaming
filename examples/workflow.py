from redsky.event_streams import Stream
from databroker.databroker import Databroker as db
import numpy as np
from pyFAI.azimuthalIntegrator import AzimuthalIntegrator as AI


def f(hdr, db, server_string, client_string):
    return next(iter(db(**{server_string: hdr[client_string]}))).restream


def load_cal(**kwargs):
    ai = AI()
    ai.setPyFAI(**kwargs)
    return


hdr = db[-1]

raw_data = hdr.restream(fill=True)
dark_data = db[hdr['sc_dk_field_uid']]
rds = Stream()
dark_data_stream = Stream()

img_stream = rds.zip(dark_data_stream
                     ).map(np.subtract
                           ).zip(air_data_stream
                                 ).map(np.subtract
                                       ).zip(cal_stream
                                             ).map(pol_correct)

mask_stream = img_stream.zip(cal_stream).map(generate_mask)

iq_stream = img_stream.zip(cal_stream, mask_stream).map(integrate)

