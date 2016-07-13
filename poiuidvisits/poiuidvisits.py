#!/usr/bin/env python

from string import Template
import pyhive


class PoiUidVisits:

    # lets do hour for now (narrow down to min later)
    MK_VISITTB = \
        Template(u' USE blklearn ; '
                 u' '
                 u' CREATE TABLE IF NOT EXISTS $tbnm '
                 u' ( '
                 u'  request_id     STRING, '
                 u'  r_timestamp    BIGINT, '
                 u'  hour           STRING, '
                 u'  pub_id         INT   , '
                 u'  tsrc_id        BIGINT, '
                 u'  lat            DOUBLE, '
                 u'  lon            DOUBLE, '
                 u'  poi            BIGINT, '
                 u'  instore        INT   , '
                 u'  onlot          INT   , '
                 u'  uid            STRING, '
                 u'  uid_type       STRING, '
                 u'  uid_hash_type  STRING, '
                 u'  user_ip        STRING, '
                 u'  sp_user_age    INT   , '
                 u'  sp_user_gender STRING, '
                 u'  age            INT   , '
                 u'  gender         STRING  '
                 u' ) '
                 u' PARTITIONED BY (cntry STRING, dt STRING) ; '
    )


    SPTL_HRLY = \
        Template(u' USE blklearn ; '
                 u' '
                 u' ADD FILE /home/canliang/nise_spatial_index_reader; '
                 u' ADD FILE /home/canliang/poiuidvisit/$idxfile; '
                 u' '
                 u' SET HIVE.EXECUTION.ENGINE=mr;'
                 u' '
                 u' DROP TABLE IF EXISTS ${tbheader}_kv ; '
                 u' '
                 u' CREATE TABLE ${tbheader}_kv AS '
                 u' SELECT * '
                 u' FROM ( SELECT TRANSFORM(T.latitude, T.longitude, T.request_id, '
                 u'                         T.r_timestamp, T.hour, T.pub_id, T.tsrc_id, '
                 u'                         T.uid, T.uid_type, T.uid_hash_type, T.user_ip, '
                 u'                         T.sp_user_age, T.sp_user_gender, T.age, T.gender, '
                 u'                         T.cntry, T.dt) '
                 u'               USING \'./nise_spatial_index_reader -i ./$idxfile -z -n 10000\' '
                 u'        FROM   ( SELECT latitude, longitude, request_id, r_timestamp, hour, pub_id, tsrc_id,'
                 u'                        uid, uid_type, uid_hash_type, user_ip, sp_user_age, sp_user_gender, age, '
                 u'                        gender, cntry, dt from default.science_core_hrly '
                 u'                 WHERE  cntry = \'us\' AND loc_score = \'tll\' AND dt = $date AND hour = $hour '
                 u'                        AND is_repeated_user AND NOT too_freq_uid '
                 u'               ) T '
                 u'      ) T1 '
                 u' WHERE SPLIT(T1.value, \'\\t\')[16] != \'-1\' ;'
        )


    PARSE_HRLY = \
        Template(u' USE blklearn ; '
                 u' '
                 u' SET hive.exec.dynamic.partition=true; '
                 u' SET hive.exec.dynamic.partition.mode=nonstrict; '
                 u' '
                 u' ADD FILE /home/canliang/poiuidvisit/$parser ; '
                 u' '
                 u' INSERT INTO TABLE $visittb PARTITION (cntry, dt) '
                 u' SELECT * FROM ( '
                 u'     SELECT TRANSFORM(key, value) '
                 u'            USING  \'python ./$parser \' '
                 u'            AS (request_id, r_timestamp, hour, pub_id, tsrc_id, '
                 u'                lat, lon, poi, instore, onlot, '
                 u'                uid, uid_type, uid_hash_type, user_ip, sp_user_age, sp_user_gender, age, gender,'
                 u'                cntry, dt) '
                 u'     FROM   ${tbheader}_kv '
                 u' ) T ; '
        )


    CLEANUP = \
        Template(u' USE blklearn ; '
                 u' '
                 u' DROP TABLE IF EXISTS ${tbheader}_kv ; ')



    def __init__(self):
        self.country = None
        self.date    = None
        self.hour    = None

        self.tbnm    = u'poiuidvisits'
        self.idxfile = u'pois2track_idx'
        self.kvparser= u'kvparser.py'

        self.pyhive  = pyhive.PyHive()

        #import pdb; pdb.set_trace()

        # query = self.MK_VISITTB.substitute(tbnm = self.tbnm)
        self.pyhive.makequery(self.MK_VISITTB.substitute(tbnm = self.tbnm))



    def sptl_hrly(self, date, hour):

        date_str      = u'\'{0}\''.format(date)
        hour_str      = u'\'{0}\''.format(hour)

        query = self.SPTL_HRLY.substitute(tbheader = self.tbnm, date = date_str, hour = hour_str, idxfile = self.idxfile)
        self.pyhive.makequery(query)


    def parse_hrly(self):

        query = self.PARSE_HRLY.substitute(visittb = self.tbnm, tbheader = self.tbnm, parser = self.kvparser)
        self.pyhive.makequery(query)


    def cleanup(self):

        self.pyhive.makequery(self.CLEANUP.substitute(tbheader = self.tbnm))



if __name__ == u'__main__':

    analyser = PoiUidVisits()

    # switched to sc3 after 02-21 data

    # should use python datetime utilities
    dates = [u'2016-02-22', u'2016-02-23', u'2016-02-24', u'2016-02-25', u'2016-02-26', u'2016-02-27', u'2016-02-28',
             u'2016-02-29', u'2016-03-01', u'2016-03-02', u'2016-03-03', u'2016-03-04', u'2016-03-05', u'2016-03-06',
             u'2016-03-07', u'2016-03-08', u'2016-03-09']


    hours = [str(x) for x in xrange(24)]

    #dates = [u'2016-01-08']
    #hours = [u'16']


    import pdb; pdb.set_trace()

    for date in dates:
        for hour in hours:
            analyser.sptl_hrly(date, hour)
            analyser.parse_hrly()

    analyser.cleanup()

