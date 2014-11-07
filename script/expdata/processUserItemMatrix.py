#!/usr/bin/env python

# Process user/item features 
##
# Input
# RID@((k1,v1)%(k2,v2))
#
# Output:
# k1 v1 k2 v2
#
# Arguments:
# minItem: inclusion criteria, watched at least how many items. 
# 

import sys

if __name__ == "__main__": 

    minItem = 5
    if len(sys.argv) > 1:
        minItem = int(sys.argv[1])

    for line in sys.stdin:
        parts = line.split("@")
        if len(parts) <2:
            continue

        rowId = int(parts[0])
        rowWatchSplit = parts[1].strip().replace("(","").replace(")","").replace(","," ").split("%")

        # we skip the users with less than `minItem` watch records. 
        if len(rowWatchSplit) < minItem:
            continue
    
        print " ".join(rowWatchSplit)


