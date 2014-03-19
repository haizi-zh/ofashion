# coding=utf-8

import json
import urlparse

__author__ = 'Ryan'

def is_chs(val):
    """
    val是否含有简体中文
    @param val:
    """
    if val:
        flag = False

        for c in val.decode('utf-8'):
            if ord(c) >= 0x4e00 and ord(c) < 0x9fa5:
                flag = True

        if flag:
            try:
                val.decode('utf-8').encode('gb2312')
            except:
                flag = False

        return flag

    return False


def is_cht(val):
    """
    val是否含有繁体中文
    @param val:
    """
    if val:
        flag = False

        for c in val.decode('utf-8'):
            if ord(c) >= 0x4e00 and ord(c) < 0x9fa5:
                flag = True

        if flag:
            flag = False

            try:
                val.decode('utf-8').encode('gb2312')
            except:
                flag = True

        return flag

    return False


def is_eng(val):
    """
    val是否为英语
    @param val:
    """
    if val:
        for c in val.decode('utf-8'):
            if ord(c) > 127:
                return False

        return True
    else:
        return False

def check_products_color(color):
    if not color:
        return True

    data = None
    try:
        data = json.loads(color)
    except:
        return False

    if isinstance(data, list):
        return True
    else:
        return False


def check_products_url(url):
    if not url:
        return False

    ret = None
    try:
        ret = urlparse.urlparse(url)
    except:
        return False

    if ret:
        eng = is_eng(url)
        if eng:
            return True
        else:
            return False
    else:
        return False

def check_products_is_local_lan(region, string):
    if region in ['cn', 'hk', 'tw', 'hk', 'us']:
        if is_chs(string) or is_cht(string) or is_eng(string):
            return True
        else:
            return False
