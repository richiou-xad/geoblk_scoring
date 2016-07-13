
#!/usr/bin/env python
import sys, json
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.5f')


def addnewscores(donescores_d, newscores_d, newscoretypeid):

    for geoblkid, scoreval in newscores_d.iteritems():
        if geoblkid not in donescores_d:
            donescores_d[geoblkid] = {}

        donescores_d[geoblkid][newscoretypeid] = scoreval

    return donescores_d


def parsedonescores(donescores_str):

    scores_d = {}

    geoblkdata_l = donescores_str.split('],')
    geoblkdata_l = [x + ']' for x in geoblkdata_l[0:len(geoblkdata_l)-1]]

    for geoblkdata in geoblkdata_l:
        geoblkid , scorestr = geoblkdata.split(':', 1)

        gblkscores_d = {}
        for elem in json.loads(scorestr):
            gblkscores_d[elem['Type']] = elem['Score']

        scores_d[geoblkid] = gblkscores_d

    return scores_d


def parsenewscores(newscores_str):
    return dict([[x.split(':')[0], x.split(':')[1]] for x in newscores_str.split(',')])


def printscores(scores_d):
    #import pdb; pdb.set_trace()
    scores_l = []
    for geoblkid, scores in scores_d.iteritems():
        newelem = [geoblkid, json.dumps([{'Type':typeid, 'Score':round(float(value),5)} for typeid, value in scores.iteritems()])]
        scores_l.append(newelem)

    print '\t'.join([geoblkid2, ','.join([':'.join(x) for x in scores_l])])




for line in sys.stdin:

    line = line.strip()

    geoblkid1, donescorestr, geoblkid2, newscoretypeid, newscorestr = line.split('\t')

    newscoretypeid = int(newscoretypeid)

    if geoblkid2 == '\\N' or geoblkid2 == 'NULL':
        print '\t'.join([geoblkid1, donescorestr])

    elif geoblkid1 == '\\N' or geoblkid1 == 'NULL' :

        donescores      = {}
        newscores       = parsenewscores(newscorestr)

        mergedscores    = addnewscores(donescores, newscores, newscoretypeid)
        printscores(mergedscores)

    else:

        donescores      = parsedonescores(donescorestr)
        newscores       = parsenewscores(newscorestr)

        mergedscores    = addnewscores(donescores, newscores, newscoretypeid)
        printscores(mergedscores)
