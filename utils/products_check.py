# coding=utf-8

import json
import urlparse
import re

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


common_html_symbol = {
    '&nbsp;': ' ',
    '&#160;': ' ',

    '&lt;': '<',
    '&#60;': '<',

    '&gt;': '>',
    '&#62;': '>',

    '&amp;': '&',
    '&#38;': '&',
}


def check_products_is_valid_string(string):
    if not string:
        return True

    processed_string = string
    try:
        for key, value in common_html_symbol.items():
            processed_string = processed_string.replace(key, value)

        processed_string = processed_string.strip()

        processed_string = re.sub(ur'&#?\w+;', '', processed_string)

        # 针对html标签的处理，感觉这一步做的有点儿过了，不必要感觉
        processed_string = re.sub(ur'<[^<>]*?>', '', processed_string)

        if processed_string == string:
            return True
        else:
            return False, processed_string
    except:
        return False
