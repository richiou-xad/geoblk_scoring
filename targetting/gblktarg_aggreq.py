#!/usr/bin/env python

import sys

curbrandid  = None
curfencetype= None
curaggreq   = 0

for line in sys.stdin:

    brandid, geoblkid, score, reqcnt, fencetype = line.strip().split('\t')

    if curbrandid is not None and curfencetype is None:

        raise ValueError

    if curbrandid is None or curbrandid != brandid or curfencetype != fencetype:

        curbrandid      = brandid
        curfencetype    = fencetype
        curaggreq       = 0

    curaggreq += int(reqcnt)

    print '\t'.join([curbrandid, geoblkid, score, str(curaggreq), curfencetype])





