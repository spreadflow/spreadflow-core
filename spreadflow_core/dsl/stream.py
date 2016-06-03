# -*- coding: utf-8 -*-

"""
Generic functions providing means to manipulate and inspect token streams.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple, OrderedDict

class StreamError(Exception):
    pass

class DuplicateTokenError(StreamError):
    pass

class NoSuchTokenError(StreamError):
    pass

AddTokenOp = namedtuple('AddTokenOp', ['token'])
SetDefaultTokenOp = namedtuple('SetDefaultTokenOp', ['token'])
RemoveTokenOp = namedtuple('RemoveTokenOp', ['token'])

def stream_extract(stream, token_class):
    tokens = list(stream)
    extracted_stream = (op for op in tokens if isinstance(op.token, token_class))
    return extracted_stream, tokens

def stream_divert(stream, token_class):
    tokens = list(stream)
    extracted_stream = (op for op in tokens if isinstance(op.token, token_class))
    remaining_stream = (op for op in tokens if not isinstance(op.token, token_class))
    return extracted_stream, remaining_stream

def token_map(stream, keyfunc=lambda op: op.token, valuefunc=lambda op: op.token):
    """
    Parses a token stream into a mapping. Order of the tokens is preserved.
    """
    present = {}
    tokens = OrderedDict()

    for op in stream:
        key = keyfunc(op)

        if isinstance(op, AddTokenOp):
            if present.get(key, False):
                raise DuplicateTokenError(key, op.token)
            else:
                present[key] = True

            tokens[key] = valuefunc(op)
        elif isinstance(op, SetDefaultTokenOp):
            tokens.setdefault(key, valuefunc(op))
        elif isinstance(op, RemoveTokenOp):
            try:
                del tokens[key]
            except KeyError:
                raise NoSuchTokenError(key, op.token)

            present[key] = False

    return tokens

def token_attr_map(stream, keyattr, valueattr=None):
    if valueattr is None:
        valueattr = keyattr
    extract_key = lambda op: getattr(op.token, keyattr)
    extract_value = lambda op: getattr(op.token, valueattr)
    return token_map(stream, extract_key, extract_value)
