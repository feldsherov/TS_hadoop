#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

from sys import stdin
from itertools import groupby
from operator import itemgetter
from numpy import array as nparray
from numpy import median, sort


re_splitter = re.compile(r"[:\t]+")

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield re_splitter.split(line.rstrip())[1:]

def main():
    data = read_mapper_output(stdin)
    countries_count = list()
    for current_year, group in groupby(data, itemgetter(0)):
        countries = sorted([country for current_year, country in group])
        countries_frequency = nparray([len(list(goup)) for country, goup in groupby(countries)])
        countries_frequency.sort()
        print "{0:<8} {1:<8} {2:<8} {3:<8} {4:<8} {5:<8} {6:<8}".format(current_year, countries_frequency.size, countries_frequency[0],
                                                    median(countries_frequency), countries_frequency[-1], \
                                                    countries_frequency.mean(), countries_frequency.std())
        

if __name__ == "__main__":
    main()