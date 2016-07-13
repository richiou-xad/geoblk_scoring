import datetime
from subprocess import call
from string import Template
import datetime
import time
import sys,os
import subprocess
import getopt
import re
import logging
import fcntl
import select
import errno
import getopt

ALGO_DEBUG = None
algo_logger = logging.getLogger(u'root')
#hp_prefix = 'hadoop fs'
hp_prefix = u'hdfs dfs'

offset=-3

def check_partition_exist(table_name, partition_criteria, query=None):
    """check if table contains a particular partition
    """
    if query == None:
        query = partition_criteria
    shell_cmd = u"hive -e \"show partitions %s partition(%s)\" | grep \"%s\" | wc | awk '{print $1}' " % (table_name, partition_criteria, query)
    #print "shell comand is : ", shell_cmd
    cmd = [u"-c", shell_cmd]
    algo_logger.info(" ".join(cmd))
    p = subprocess.Popen(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
    out, err = p.communicate()
    #print out.strip()
    if p.returncode != 0:
        return 0
    if int(out.strip()) > 0:
        return 1
    return 0


def main(argv):
    r = re.compile('.*-.*-.*')
    if len(argv)>0:
        date = str(argv[0])
        if r.match(date):
            compare_date = date
        else:
            print u'input date is invalid! the valid format is yyyy-mm-dd.  start to use default date: yesterday...'
            compare_date = (datetime.date.today() + datetime.timedelta(offset)).strftime(u"%Y-%m-%d")
    else:
        print u'no input date, start to use default date: yesterday'
        compare_date = (datetime.date.today() + datetime.timedelta(offset)).strftime(u"%Y-%m-%d")
    
    partition_query= u'dt=\\\"' + compare_date+ u'\\\"'
    #partition_query = 'dt=\\\"'+compare_date+'\\\"'
    if check_partition_exist(u'ex_addetails_hrly',partition_query, compare_date):
        print u'partitions exist!'
        print u"start to analyze polygon ctr daily on " , compare_date, u"..."
        country = u'us'
        #location_scores = ['95+','94','93','92-']
        SLL_scores = [90]
        for SLL_score in SLL_scores:
            if SLL_score > 94:
                location_score = u'95+'
            elif SLL_score == 94:
                location_score = u'94'
            elif SLL_score == 93:
                location_score = u'93'
            elif SLL_score <93 :
                location_score = u'92-'
        #for compare_hour in range(24):
            call(u'hive -f $POLYGON_HOME/polygon_ctr_analysis1.hql -hiveconf compare_date=' + compare_date +
                 u' -hiveconf country=' + country +
                 u' -hiveconf location_score=' + location_score +
                 u' -hiveconf SLL_score=' + str(SLL_score) , shell=True)
    else:
        print partition_query, u" does not exist. skip this partition."

if __name__ == u"__main__":
    sys.exit(main(sys.argv[1:]))


