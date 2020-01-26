# -*- coding: utf-8 -*-
"""
Shortcuts extention for SQL-Tools lib (pretty) (shortcuts)
"""

from .parser import parse
from .styler import style
from .tokenizer import tokenize
from .util import print_non_data


def format_sql(s, debug=False):
    tokens = list(tokenize(s))
    if debug:
        print_non_data('Tokens: %s' % tokens)
    parsed = list(parse(tokens))
    if debug:
        print_non_data('Statements: %s' % parsed)
    styled = style(parsed)
    if debug:
        print_non_data('Output: %s' % styled)
    return styled
