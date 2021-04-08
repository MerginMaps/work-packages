"""
Module with useful utilities.
"""


def escape_double_quotes(name):
    escaped_name = name.replace('"', '""')
    quoted_name = f'"{escaped_name}"'
    return quoted_name
