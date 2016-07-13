from   string import Template
from datetime import datetime, timedelta
from subprocess import call


class AdReqLean:

    MAKETABLE = Template(
        u' USE blklearn; '
        u' set mapred.job.queue.name = canliang ; '
        u' set tez.queue.name=canliang ; '
        u' '
        u' CREATE TABLE IF NOT EXISTS $dsttb '
        u' ( '
        u'  r_timestamp      BIGINT, '
        u'  pub_id           INT   , '
        u'  tsrc_id          BIGINT, '
        u'  lat              DOUBLE, '
        u'  lon              DOUBLE, '
        u'  uid              STRING, '
        u'  is_repeated_user BOOLEAN, '
        u'  too_freq_uid     BOOLEAN '
        u' ) '
        u' PARTITIONED BY (cntry STRING, dt STRING, hour STRING) ; '
    )

    # find request_id to block_id mapping
    FILLHRLY = Template(
        u' USE blklearn ; '
        u' SET mapred.job.queue.name = canliang ; '
        u' SET tez.queue.name=canliang ; '
        u' SET hive.exec.dynamic.partition=true ; '
        u' SET hive.exec.dynamic.partition.mode=nonstrict ; '
        u' '
        u' INSERT INTO TABLE $dsttb '
        u' PARTITION(cntry, dt, hour) '
        u' SELECT r_timestamp, pub_id, tsrc_id, latitude, longitude, uid, is_repeated_user, too_freq_uid, '
        u'        cntry, dt, hour '
        u' FROM   default.science_core_hrly '
        u' WHERE  cntry = ${cntry} AND loc_score = \'tll\' AND dt = $date AND hour = $hour ; '
    )


    def __init__(self):

        self.tbname  = u'adreqleanv2'


        query = AdReqLean.MAKETABLE.substitute(dsttb = self.tbname)
        self.queryhive(query)


    def queryhive(self, query):

        query = u''.join([u' SET mapred.job.queue.name = canliang ; ', u' SET tez.queue.name=canliang ; ',
                          u' USE blklearn; ', query])

        call([u'hive', u'-e', u'{0}'.format(query)])



    def fillhrly(self, cntry, date, hour):

        cntrystr     = u'\'{0}\''.format(cntry)
        datestr      = u'\'{0}\''.format(date)
        hourstr      = u'\'{0}\''.format(hour)

        query = AdReqLean.FILLHRLY.substitute(dsttb = self.tbname, cntry = cntrystr, date = datestr, hour = hourstr)
        self.queryhive(query)



if __name__ == u'__main__':

    cntry = u'gb'
    op = AdReqLean()

    day     = datetime(2016,6,11)
    lastday = datetime(2016,7,10)
    step    = timedelta(days=1)
    import pdb; pdb.set_trace()
    while day <= lastday:
        date  = day.strftime('%Y-%m-%d')
        hours = [str(x) for x in xrange(24)]
        for hour in hours:
            op.fillhrly(cntry, date, hour)
        day = day + step
