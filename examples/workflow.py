from redsky.streams import Stream
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
rds = Stream()

dark_data_stream = rds.query_db(db=db,
                                server_string='dark_server_uid',
                                client_string='dark_client_uid')

air_data_stream = rds.query_db(db=db,
                               server_string='air_server_uid',
                               client_string='air_client_uid')

cal_stream = rds.query_db(db=db,
                          server_string='detector_calibration_server_uid',
                          client_sring='detector_calibration_client_uid'
                          ).map(load_cal)

img_stream = rds.zip(dark_data_stream
                     ).map(np.subtract
                           ).zip(air_data_stream
                                 ).map(np.subtract
                                       ).zip(cal_stream
                                             ).map(pol_correct)

mask_stream = img_stream.zip(cal_stream).map(generate_mask)

iq_stream = img_stream.zip(cal_stream, mask_stream).map(integrate)
