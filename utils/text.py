# coding=utf-8

"""
一些文本处理的函数
"""
import types

__author__ = 'Zephyre'


def iterable(val):
    """
    val是否iterable。注意：val为str的话，返回False。
    :param val:
    """
    if isinstance(val, types.StringTypes):
        return False
    else:
        try:
            iter(val)
            return True
        except TypeError:
            return False


def unicodify(val):
    """
    Unicode化，并且strip
    :param val:
    :return:
    """
    if val is None:
        return None
    elif isinstance(val, str):
        return val.decode('utf-8').strip()
    else:
        return unicode(val).strip()
