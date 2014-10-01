#!/usr/bin/env python

# Transform feature matrix data.
#
# Input
# RID@((k1,v1)%(k2,v2))
#
# Output:
# Line Number: k1 v1 k2 v2
#
#

import sys

if __name__ == "__main__": 

    featureMap = {}
    maxIndex   = -1

    for line in sys.stdin:
        parts = line.split("@")
        if len(parts) <2:
            continue

        rowId = int(parts[0])
        rowFeatureLine = parts[1].replace("(","").replace(")","").replace(","," ")
        
        featureMap[rowId] = rowFeatureLine

        if rowId > maxIndex:
            maxIndex = rowId

    #print "max index:" + str(maxIndex)
    #print "map:" + str(featureMap)

    # start to output. 
    if maxIndex > -1:
        for i in range(1, maxIndex+1):
            #print i
            if i in featureMap:
                print " ".join(featureMap[i].strip().split("%"))
            else:
                print " "


