# coding=utf-8
import types
import _mysql
from scrapy import signals
from scrapy.exceptions import NotConfigured

__author__ = 'Zephyre'


def unicodify(val):
    if isinstance(val, str):
        return val.decode('utf-8')
    else:
        return val


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


def product_tags_merge(src, dest):
    """
    合并两个tag列表：把src中的内容合并到dest中
    :param src:
    :param dest:
    """
    def to_set(val):
        """
        如果val是iterable，则转为set，否则……
        :param val:
        :return:
        """
        return set(val) if iterable(val) else {val}

    dest = {k: to_set(dest[k]) for k in dest if dest[k]}
    src = {k: to_set(src[k]) for k in src if src[k]}

    for k in src:
        if k not in dest:
            dest[k] = src[k]
        else:
            dest[k] = dest[k].union(src[k])

    # 整理
    return dict((k, list(dest[k])) for k in dest)
