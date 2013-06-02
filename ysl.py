# coding=utf-8

__author__ = 'Zephyre'

import json
import string
import urllib
import urllib2
import re
import common

url = 'http://www.ysl.com/en_US/stores'


def get_store_details(html, opt):
    stores = []
    for m in re.findall('<li>\s*?<address>\s*?<h2>(.*?)</h2><br/>(.*?)</address>\s*?</li>', html, re.S):
        store_name = m[0].strip()
        addr_str = m[1].strip()

        spl = addr_str.split('<br/>')
        store_type = spl[0].strip()
        store_hour = spl[-2].strip()
        store_tel = spl[-3].strip()
        store_addr = '\r\n'.join([val.strip() for val in spl[1:-3]])

        stores.append(
            {'name': store_name, 'addr': store_addr, 'type': store_type, 'opening': store_hour, 'tel': store_tel})
        print('Found store: %s, %s, %s, %s, %s' % (store_name, store_addr, store_tel, opt['country'], opt['continent']))
    return stores


def get_entries(html, level, opt):
    pat_list = ['countries', 'cities', 'stores']
    pat = r'<li><span>([\w\s]+?)</span>\s+<ul class="%s">' % pat_list[level]

    entries = [m for m in re.findall(pat, html, re.S | re.U)]

    # 分成若干洲片段
    con_split = [html.find(pat) for pat in [r'<li><span>%s</span>' % con for con in entries]]
    con_split.append(-1)
    con_map = {} # {'America':'html sub str'}
    for i in xrange(con_split.__len__() - 1):
        con_map[entries[i]] = html[con_split[i]:con_split[i + 1]]

    stores = []
    # 进入下一级
    if level == 2:
        # 获得最终的店铺信息
        for key in con_map:
            val = con_map[key]
            opt['city'] = key
            stores.extend(get_store_details(val, opt))
    else:
        for key in con_map:
            val = con_map[key]
            if level == 0:
                opt = {'continent': key}
            elif level == 1:
                opt['country'] = key
            stores.extend(get_entries(val, level + 1, opt))

    return stores


def fetch():
    html = common.get_data(url)
    continents = get_entries(html, 0, None)
    print continents





