# -*- coding: utf-8 -*-
"""
Utility extention for SQL-Tools lib (pretty) (utility)
"""
from __future__ import print_function

import sys


def print_data(msg):
    print(msg)


def print_non_data(msg):
    print(msg, file=sys.stderr)
