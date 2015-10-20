#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

def main():
    for rw in sys.stdin:
        if rw.startswith("\""):
            continue
        key, val = rw.strip().split(",")
        print "%s\t%s" % (key, val)

if __name__ == "__main__":
    main()