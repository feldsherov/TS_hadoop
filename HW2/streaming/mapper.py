#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

def main():
    for rw in sys.stdin:
        if rw == "":
            continue
        key, pgr, edg_r = rw.strip("\n").split("\t")
        pgr = float(pgr)
        if edg_r != "":
            print "%s\t\t%s" % (key, edg_r)
        edgs = edg_r.split()
        for e in edgs:
            print "%s\t%s\t" % (e, pgr / len(edgs))

if __name__ == "__main__":
    main()