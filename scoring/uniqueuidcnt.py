
#!/usr/bin/env python

import sys


def evalscore(totviscnt, bktdata):

    bktdata_l = [[int(x) for x in abkt.split(':')] for abkt in bktdata.split('|')]
    bktdata_l.sort(key=lambda x:x[1], reverse=True)

    if len(bktdata_l) == 0:
        return None

    maxscore  = float(bktdata_l[0][1]) / totviscnt
    bktscores = []
    for ablkdata in bktdata_l:
        uidcnt  = ablkdata[1]
        score   = float(uidcnt) / totviscnt
        if uidcnt > 1 : # and uidcnt > 0.01*totviscnt and score >= 0.2 * maxscore:
            bktscores.append([ablkdata[0], score])

    if len(bktscores) > 0:
        bktscores_o     = bktscores[0:min(50, len(bktscores))]
        bktscoresstr    = u','.join([u':'.join([str(x),str(y)]) for x,y in bktscores_o])
    else:
        bktscoresstr    = None
    return bktscoresstr

for line in sys.stdin:

    line = line.strip()

    poiid, totviscnt, bktdata = line.split('\t')

    if poiid != '\\N' and int(totviscnt) > 20 :
        bktscoresstr = evalscore(int(totviscnt), bktdata)
        if bktscoresstr is not None:
            print '\t'.join([poiid, bktscoresstr])
