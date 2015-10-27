#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

import re
import sys

from itertools import groupby
from operator import itemgetter

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip("\n").split(separator, 1)


def main():
    data = read_mapper_output(sys.stdin)
    for current_key, group in groupby(data, itemgetter(0)):
        f = False
        to, cpg = "", 0
        for val in map(itemgetter(1), group):
            pr, curr_to = val.split("\t")
            if curr_to == "":
                cpg += float(pr)
            else:
                to = curr_to
        
        cpg *= 0.85
        cpg += 0.15
        print "%s\t%s\t%s" % (current_key, cpg, to)

if __name__ == "__main__":
    main()