#!/usr/bin/env python

import sys


for line in sys.stdin:

    line = line.strip()

    elems = line.split('\t')

    lat = elems[0]
    lon = elems[1]
    request_id = elems[2]
    r_timestamp = elems[3]
    hour = elems[4]
    pub_id = elems[5]
    tsrc_id = elems[6]
    uid = elems[7]
    uid_type = elems[8]
    uid_hash_type = elems[9]
    user_ip = elems[10]
    sp_user_age = elems[11]
    sp_user_gender = elems[12]
    age = elems[13]
    gender = elems[14]
    cntry = elems[15]
    dt = elems[16]

    pids_l   = elems[17:len(elems)]

    pois = {}
    offset = 1000000000
    for pid in pids_l:
        pid_num = int(pid)
        if pid_num < offset :
            if pid_num in pois.keys():
                pois[pid_num][0] = u'1'
            else:
                pois[pid_num] = [u'1', u'0']

        if pid_num >= offset :
            pid_num = pid_num - offset
            if pid_num in pois.keys():
                pois[pid_num][1] = u'1'
            else:
                pois[pid_num] = [u'0', u'1']



    for poi, vis in pois.iteritems():
        #import pdb; pdb.set_trace()
        print u'\t'.join([request_id, r_timestamp, hour, pub_id, tsrc_id,
                          lat, lon, unicode(poi), vis[0], vis[1],
                          uid, uid_type, uid_hash_type, user_ip, sp_user_age, sp_user_gender, age, gender,
                          cntry, dt])


