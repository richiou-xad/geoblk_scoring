#!/usr/bin/env python

import sys, copy


def evalscore(brandid, poi, totviscnt, bktdata, method = 'totuidnormed'):

    if totviscnt <= 20 :
        return

    # filter out bad ones
    filteredblkdata = [[x,y[0],y[1]] for x,y in bktdata.iteritems() if y[0]>2 and y[0] > 0.01*totviscnt and y[1]>20]

    newblkdata = []
    for ablkdata in filteredblkdata:
        newdata = copy.deepcopy(ablkdata)
        newdata.append(float(ablkdata[1])/totviscnt)
        newdata.append(float(ablkdata[1])/ablkdata[2])

        newblkdata.append(newdata)

    if len(newblkdata) == 0:
        return

    maxviscvg   = max([x[3] for x in newblkdata])
    maxcontrast = max([x[4] for x in newblkdata])

    tmpl = [x[4] for x in newblkdata if x[4] < maxcontrast]
    if len(tmpl) == 0:
        secmaxcontrast = maxcontrast
    else:
        secmaxcontrast = max([x[4] for x in newblkdata if x[4] < maxcontrast])

    newblkdata  = [x for x in newblkdata if x[3] > 0.10 * maxviscvg]
    newblkdata  = [x for x in newblkdata if x[4] > 0.15 * secmaxcontrast]
    if len(newblkdata) == 0:
        return

    newblkdata.sort(key=lambda x: x[4], reverse = True)
    newblkdata_o    = newblkdata[0:min(50, len(newblkdata))]
    #newblkdata_o    = newblkdata # for targetting, get all the geoblks relevant
    newblkdata_str  = u','.join([u':'.join([str(x[0]),str(x[4]/maxcontrast)]) for x in newblkdata_o])

    print '\t'.join([poi, newblkdata_str, brandid])



curpoi          = None
curbrand        = None
curtotviscnt    = 0
curpoibktdata   = {}



for line in sys.stdin:


    line    = line.strip()

    brandid, poiid, totviscnt, geoblkid, blkviscnt, blkuidcnt = line.split('\t')

    if poiid != '\\N' and brandid != '\\N':

        if curpoi is None :
            curpoi          = poiid
            curbrand        = brandid
            curtotviscnt    = float(totviscnt)
            curpoibktdata   = {}

        elif curpoi != poiid :
            evalscore(curbrand, curpoi, curtotviscnt, curpoibktdata)

            curpoi          = poiid
            curbrand        = brandid
            curtotviscnt    = float(totviscnt)
            curpoibktdata   = {}

        curpoibktdata[geoblkid] = [float(blkviscnt), float(blkuidcnt)]

if curpoi is not None:
    evalscore(curbrand, curpoi, curtotviscnt, curpoibktdata)
