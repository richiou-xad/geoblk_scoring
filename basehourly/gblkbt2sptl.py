
from string import Template
from subprocess import call



class MmbSptl:


    MK_UIDPIDHRLY = \
        Template(u' USE blklearn; '
                 u' '
                 u' CREATE TABLE IF NOT EXISTS locvisual_uidpid_hlry_gblkcatmgr ( '
                 u' geoblkid INT, uid   STRING, reqcnt INT )   '
                 u' PARTITIONED BY (hour  STRING, dt    STRING ) ; ')


    # lets do hour for now
    COL_UIDPID_HRLY = \
        Template(u' USE blklearn; '
                 u' '
                 u' ADD FILE /home/canliang/nise_spatial_index_reader; '
                 u' ADD FILE /home/canliang/locvisual/$idxfile; '
                 u' SET hive.exec.dynamic.partition=true; '
                 u' SET hive.exec.dynamic.partition.mode=nonstrict; '
                 u' '
                 u' INSERT INTO TABLE locvisual_uidpid_hlry_gblkcatmgr PARTITION (hour, dt) '
                 u' SELECT T2.pid, T2.uid, COUNT(*) AS reqcnt, $hour, $date FROM '
                 u'     ( SELECT T1.uid, T1.pid FROM '
                 u'          ( SELECT TRANSFORM(T0.lat, T0.lon, T0.uid) '
                 u'                   USING \'./nise_spatial_index_reader -i ./$idxfile -z -n 1\' '
                 u'                   AS (lat, lon, uid, pid) '
                 u'            FROM   adreqlean T0 '
                 u'            WHERE  cntry = \'us\' AND dt = $date AND hour = $hour AND NOT too_freq_uid '
                 u'          ) T1 '
                 u'       WHERE T1.pid != \'-1\' '
                 u'     ) T2 '
                 u' GROUP BY T2.uid, T2.pid ; '
        )

    # should also include number of hours
    TRAN_HRLY2WEEKLY = \
        Template(u' USE blklearn; '
                 u' '
                 u' INSERT INTO TABLE locvisual_uidpid_weekly_gblkcatmgr PARTITION (weekkcode = $weekcode) '
                 u' SELECT geoblkid, uid, SUM(reqcnt) '
                 u' FROM   locvisual_uidpid_hlry_gblkcatmgr '
                 u' WHERE  dt > $stdate AND st <= $eddate '
                 u' GROUP  BY geoblkid, uid ; ')



    def __init__(self):
        self.queryhive(self.MK_UIDPIDHRLY.substitute())


    def queryhive(self, query):

        query = u''.join([u' set mapred.job.queue.name = canliang ; ', u' set tez.queue.name=canliang ; ',
                          u' USE blklearn; ', query])

        call([u'hive', u'-e', u'{0}'.format(query)])



    def prochrly(self, hour, date):

        idxfile = u'gblkcatmgr_us_idx'
        datestr = u'\'{0}\''.format(date)
        hourstr = u'\'{0}\''.format(hour)

        query = self.COL_UIDPID_HRLY.substitute(idxfile = idxfile, hour = hourstr, date = datestr)
        self.queryhive(query)


    def procday(self, date):
        datestr = u'\'{0}\''.format(date)
        for hour in xrange(24):
            self.prochrly(hour=hour, date=date)

        query = self.TRAN_HRLY2DAILY.substitute(date = datestr)
        self.queryhive(query)




if __name__ == u'__main__':

    mmbsptl = MmbSptl()

    dates = [u'2016-04-22', u'2016-04-23', u'2016-04-24', u'2016-04-25', u'2016-04-26', u'2016-04-27', u'2016-04-28',
             u'2016-04-15', u'2016-04-16', u'2016-04-17', u'2016-04-18', u'2016-04-19', u'2016-04-20', u'2016-04-21']
    for date in dates:
        mmbsptl.procday(date)

