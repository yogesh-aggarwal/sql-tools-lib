# -*- coding: utf-8 -*-
"""
Makes SQL query readable for SQL-Tools lib (pretty) (styler)
"""
from .parser import (Between, Case, Condition, Else, From, Func,
                               GroupBy, Having, Identifier, Insert, InvalidSQL,
                               Is, Join, Limit, Link, Not, Null, Number, On,
                               Operator, OrderBy, Select, Semicolon, Str,
                               SubSelect, When, Where)


def types_match(condition, types_list):
    if len(condition.values) != len(types_list):
        return False

    for value, types in zip(condition.values, types_list):
        if not isinstance(value, types):
            return False
    return True


class Liner:

    def __init__(self):
        self.line = []
        self.lines = []

    def add_to_line(self, val):
        self.line.append('%s' % val)

    def add_line(self, val):
        self.add_to_line(val)
        self.end_line()

    def add_empty_lines(self, count=1):
        self.end_line()
        for _ in range(count):
            self.lines.append('')

    def end_line(self):
        line = ''.join(self.line)
        if line:
            self.lines.append(line)
        self.line = []

    def add_to_last_line(self, val):
        line = self.lines.pop()
        self.add_to_line(line)
        self.add_to_line(val)
        self.end_line()


def _style_identifier(identifier, liner, end_line=True):
    liner.add_to_line(identifier)

    if identifier.as_:
        liner.add_to_line(' AS')

    if identifier.alias:
        liner.add_to_line(' ')
        liner.add_to_line(identifier.alias)

    if end_line:
        liner.end_line()


def _style_from(from_, liner, indent):
    liner.add_line('    ' * indent + 'FROM')

    i = 0
    while i < len(from_.values):
        value = from_.values[i]

        if isinstance(value, Join):
            liner.add_to_line('    ' * (indent + 1))
            liner.add_to_line(value.value.upper())
            liner.add_to_line(' ')

            _style_identifier(from_.values[i + 1], liner, end_line=False)

            if i + 2 < len(from_.values) and isinstance(from_.values[i + 2], On):
                liner.add_to_line(' ON')

            i += 1

        elif isinstance(value, Identifier):
            liner.add_to_line('    ' * (indent + 1))
            _style_identifier(value, liner, end_line=False)

            if i + 1 < len(from_.values) and not isinstance(from_.values[i + 1], Join):
                liner.add_to_line(',')

        elif isinstance(value, On):
            new_line = True

            for v in value.values:
                if new_line:
                    liner.add_to_line('    ' * (indent + 2))

                if isinstance(v, Link):
                    liner.add_to_line('AND ')
                    new_line = False

                elif isinstance(v, Condition):
                    for vv in v.values:
                        if isinstance(vv, Func):
                            _style_func(vv, liner, end_line=False)
                        elif isinstance(vv, Identifier):
                            liner.add_to_line('%s' % vv)
                        else:
                            liner.add_to_line(' %s ' % vv)

                    liner.end_line()
                    new_line = True

            i += 1

        liner.end_line()
        i += 1


def _style_group_by(group_by, liner, indent):
    liner.add_line('    ' * indent + 'GROUP BY')

    for i, value in enumerate(group_by.values):
        liner.add_to_line('    %s' % value)
        if i + 1 < len(group_by.values):
            liner.add_to_line(',')
        liner.end_line()

    if group_by.with_rollup:
        liner.add_line('    ' * (indent + 1) + 'WITH ROLLUP')


def _style_limit(limit, liner, indent):
    if all([limit.row_count, limit.offset, limit.offset_keyword]):
        line = 'LIMIT %s OFFSET %s' % (limit.row_count.value,
                                       limit.offset.value)
    elif all([limit.row_count, limit.offset]):
        line = 'LIMIT %s, %s' % (limit.offset.value, limit.row_count.value)
    else:
        line = 'LIMIT %s' % limit.row_count.value

    liner.add_line('    ' * indent + line)


def _style_order_by(order_by, liner, indent):
    liner.add_line('    ' * indent + 'ORDER BY')

    for i, value in enumerate(order_by.values):
        liner.add_to_line('    ' * (indent + 1) + '%s' % value)

        if value.sort:
            sort = value.sort.upper()
            liner.add_to_line(' %s' % sort)

        if i + 1 < len(order_by.values):
            liner.add_to_line(',')

        liner.end_line()


def _style_having(having, liner, indent):
    liner.add_line('    ' * indent + 'HAVING')

    i = 0
    while i < len(having.values):
        liner.add_to_line('    ' * (indent + 1))

        if isinstance(having.values[i], Link):
            liner.add_to_line('%s ' % having.values[i].value.upper())
            i += 1

        condition = having.values[i]

        if types_match(condition, [Identifier, Operator, Number]):
            liner.add_to_line(' '.join('%s' % x for x in condition.values))
            i += 1
            liner.end_line()

        if types_match(condition, [Not, Func, Operator, Number]):
            liner.add_to_line('NOT')
            liner.add_to_line(' ')
            _style_func(condition.values[1], liner, end_line=False)
            liner.add_to_line(' ')
            liner.add_to_line(condition.values[2])
            liner.add_to_line(' ')
            liner.add_to_line(condition.values[3])
            i += 1

    liner.end_line()


def _style_condition(condition, liner, indent):

    if len(condition.values) == 4 and isinstance(condition.values[0], Not):
        liner.add_to_line('NOT ')
        condition.values.pop(0)

    if types_match(condition, [(Identifier, Number, Str),
                               Operator,
                               (Identifier, Number, Str)]):

        liner.add_to_line(' '.join('%s' % x for x in condition.values))

    elif types_match(condition, [(Identifier, Number, Str),
                                 Between,
                                 (Identifier, Number, Str),
                                 Link,
                                 (Identifier, Number, Str)]):
        liner.add_to_line('%s BETWEEN %s AND %s' % (condition.values[0],
                                                    condition.values[2],
                                                    condition.values[4]))

    elif types_match(condition, [(Identifier, Number, Str), Is, Null]):
        liner.add_to_line('%s IS NULL' % condition.values[0])

    elif types_match(condition, [(Identifier, Number, Str), Is, Not, Null]):
        liner.add_to_line('%s IS NOT NULL' % condition.values[0])

    elif types_match(condition, [(Identifier, Number, Str), Operator, list]):
        liner.add_to_line('%s IN (' % condition.values[0])
        liner.end_line()

        for j, value in enumerate(condition.values[2]):
            liner.add_to_line('    ' * (indent + 2))

            liner.add_to_line(value)
            if j + 1 < len(condition.values[2]):
                liner.add_to_line(',')

            else:
                liner.add_to_line(')')
            liner.end_line()

    elif types_match(condition, [Identifier, Operator, SubSelect]):
        liner.add_to_line('%s %s (' % (condition.values[0],
                                       str(condition.values[1]).upper()))
        liner.end_line()
        style(condition.values[2].values, liner=liner, indent=indent + 2)
        liner.add_to_last_line(')')

    liner.end_line()


def _style_where(where, liner, indent):
    liner.add_line('    ' * indent + 'WHERE')

    i = 0
    while i < len(where.conditions):
        liner.add_to_line('    ' * (indent + 1))
        if isinstance(where.conditions[i], Link):
            liner.add_to_line('%s ' % where.conditions[i].value.upper())
            i += 1

        _style_condition(where.conditions[i], liner, indent)
        i += 1


def _style_func(func, liner, end_line=True):
    liner.add_to_line(func.name.upper())
    liner.add_to_line('(')

    for i, arg in enumerate(func.args):
        if isinstance(arg, (Identifier, Str, Number)):
            liner.add_to_line(arg)
        elif isinstance(arg, Func):
            _style_func(arg, liner, end_line=False)

        if i + 1 < len(func.args):
            liner.add_to_line(', ')

    liner.add_to_line(')')
    if func.as_ and func.alias:
        liner.add_to_line(' AS ')
        liner.add_to_line(func.alias)

    if end_line:
        liner.end_line()


def _style_case(case, liner, indent):
    liner.add_line(case.value.upper())
    for i, when_else in enumerate(case.when_elses):
        liner.add_to_line('%s%s' % ('    ' * (indent + 2), when_else))

        if i < len(case.when_elses) - 1:
            liner.end_line()


def _style_insert(insert, liner, indent):
    liner.add_line('INSERT INTO')
    liner.add_line('    %s' % insert.table)
    if insert.cols:
        liner.add_to_last_line(' (%s)' %
                               ', '.join('%s' % x for x in insert.cols))

    if insert.values:
        liner.add_line('VALUES')
        for i, values in enumerate(insert.values.values):
            liner.add_line('    (%s)' % ', '.join('%s' % x for x in values))

            if i < len(insert.values.values) - 1:
                liner.add_to_last_line(',')

    elif insert.select:
        style(insert.select, liner=liner, indent=indent)


def _style_select(select, liner, indent):
    liner.add_line('    ' * indent + select.value.upper())

    for i, value in enumerate(select.values):
        liner.add_to_line('    ' * (indent + 1))
        if isinstance(value, (Identifier, Str, Number)):
            _style_identifier(value, liner, end_line=False)

        elif isinstance(value, Case):
            _style_case(value, liner, indent)

        elif isinstance(value, Func):
            _style_func(value, liner, end_line=False)

        if i + 1 < len(select.values):
            liner.add_to_line(',')
        liner.end_line()


def _style_semicolon(semicolon, liner, indent):
    liner.add_to_last_line(semicolon.value)


def style(statements, indent=0, keyword_upper=True, liner=None):
    if not liner:
        liner = Liner()

    structures = {
        From: _style_from,
        GroupBy: _style_group_by,
        Having: _style_having,
        Insert: _style_insert,
        Limit: _style_limit,
        OrderBy: _style_order_by,
        Select: _style_select,
        Semicolon: _style_semicolon,
        Where: _style_where,
    }

    for i, statement in enumerate(statements):
        func = structures[statement.__class__]
        try:
            func(statement, liner=liner, indent=indent)
        except IndexError:
            raise InvalidSQL("Not a valid SQL query")

        if isinstance(statement, Semicolon) and len(statements) > i + 1:
            # statements holds multiple separate statements
            liner.add_empty_lines(count=2)

    liner.end_line()
    return liner.lines
