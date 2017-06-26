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

from ..streamer import Doc


def test_Doc(exp_db, start_uid1):
    ih1 = exp_db[start_uid1]
    s = exp_db.restream(ih1)
    d = Doc(input_info=[('img', 'pe1_image')], output_info=[
        ('img2', {'dtype': 'array', 'source': 'testing'})])
    for a in s:
        print(a)
        if a[0] == 'event':
            doc = a[1]
            guts = d.event_guts(doc)['img']
            print(d.issue_event(guts))
        else:
            print(d.dispatch(a))
