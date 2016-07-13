u"""
for each poi, collect its visitor's behavior around it (distance bucket and bucket frequency)

For generality, it's better to always expand to pairs first and then filter if necessary and then aggregate
u"""

from subprocess import call
from string import Template


class Poi2GeoblkHtry:


    MK_POIUID = Template(
        u' USE blklearn; '
        u' '
        u' CREATE TABLE IF NOT EXISTS locvisual_poiuid AS  '
        u' SELECT T1.brandid, T1.poiid, T2.uid '
        u' FROM adarad_brdpoi T1 JOIN poiloyal T2 ON T1.poiid = T2.poi; '
    )



    SCORE = Template(
        u' USE blklearn; '
        u' '
        u' DROP TABLE IF EXISTS locvisual_poigeoblkscore_$targcaseid ; '
        u' CREATE TABLE locvisual_poigeoblkscore_$targcaseid (poiid INT, blkscores STRING) ; '
        u' '
        u' ADD FILE /home/canliang/locvisual/scoring/$scorefun ; '
        u' '
        u' INSERT INTO TABLE locvisual_poigeoblkscore_$targcaseid '
        u' SELECT * '
        u' FROM ( SELECT TRANSFORM(T1.poiid, T2.totviscnt, T1.blkdata) '
        u'        USING \'python ./$scorefun \' '
        u'        AS (poiid, blkscores) '
        u'        FROM locvisual_poigeoblkid_$srccaseid T1 '
        u'             JOIN '
        u'             (select poi as poiid, count(*) as totviscnt from poiloyal group by poi) T2 '
        u'             ON T1.poiid = T2.poiid '
        u'      ) T ; '
    )



    def __init__(self):
        query = self.MK_POIUID.substitute()
        self.queryhive(query)


    def queryhive(self, query):

        query = u''.join([u' SET mapred.job.queue.name = canliang ; ', u' SET tez.queue.name=canliang ; ',
                          u' USE blklearn; ', query])

        call([u'hive', u'-e', u'{0}'.format(query)])


    def score(self, targcaseid, srccaseid):

        query = self.SCORE.substitute(targcaseid=targcaseid, srccaseid = srccaseid, scorefun = u'uniqueuidcnt.py')
        self.queryhive(query)


    # hive -e 'select T1.hashkey, T2.blkscores from blklearn.poi_id2hash T1 join blklearn.locvisual_poigeoblkscore_trial T2 on T1.poiid = T2.poiid; ' > poi2geoblkscore.txt
    # hive -e 'select * from blklearn.locvisual_brdpoigeoblkscore_trial WHERE hashkey is not null and blkscores is not null and brandid is not null ; ' > poi2geoblkscore.txt


if __name__ == u'__main__':

    analyser = Poi2GeoblkHtry()

    analyser.score(u'gblkcatmgr_0215_0410', u'gblkcatmgr_0215_0410')
    #analyser.score(u'gblkcatmgr_0215_0313', u'gblkcatmgr_0215_0313')
    #analyser.score(u'gblkcatmgr_0314_0410', u'gblkcatmgr_0314_0410')
