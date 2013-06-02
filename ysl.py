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
        store_addr = common.reformat_addr('\r\n'.join([val.strip() for val in spl[1:-3]]))

        stores.append(
            {'continent': opt['continent'], 'city': opt['city'], 'country': opt['country'], 'name': store_name,
             'addr': store_addr, 'type': store_type, 'opening': store_hour, 'tel': store_tel})
        print('Found store: %s | %s | %s | (%s | %s)' % (store_name, store_addr, store_tel, opt['country'], opt['continent']))
    return stores


def get_entries(html, pat):
    entries = [m for m in re.findall(pat, html, re.S | re.U)]

    # 分成若干洲片段
    con_split = [html.find(pat) for pat in [r'<li><span>%s</span>' % con for con in entries]]
    con_split.append(-1)
    con_map = {} # {'America':'html sub str'}
    for i in xrange(con_split.__len__() - 1):
        con_map[entries[i]] = html[con_split[i]:con_split[i + 1]]

    return con_map

def get_continents(url):
    pat = ur'<li><span>([\w\s]+?)</span>\s+<ul class="countries">'
    return get_entries(common.get_data(url), pat)


def get_countries(html):
    pat = ur'<li><span>([\w\s]+?)</span>\s+<ul class="cities">'
    return get_entries(html, pat)


def get_cities(html):
    pat = ur'<li><span>([\w\s]+?)</span>\s+<ul class="stores">'
    return get_entries(html, pat)


def fetch():
    def func(o_data, level):
        if level == 4:
            # 返回商店信息
            stores = get_store_details(o_data['content'],
                                       {'city': o_data['city'], 'country': o_data['country'],
                                        'continent': o_data['continent']})
            return [{'func': None, 'data': s} for s in stores]
        else:
            func_map = {1: get_continents, 2: get_countries, 3: get_cities}
            if level == 1:
                param = o_data['url']
            else:
                param = o_data['content']

            entries = func_map[level](param)
            siblings = []
            for ent in entries:
                if level == 1:
                    data = {'continent': ent, 'content': entries[ent]}
                elif level == 2:
                    data = {'country': ent, 'content': entries[ent], 'continent': o_data['continent']}
                elif level == 3:
                    data = {'city': ent, 'content': entries[ent], 'country': o_data['country'],
                            'continent': o_data['continent']}
                siblings.append({'func': lambda data: func(data, level + 1), 'data': data})
            return siblings

    node = {'func': lambda data: func(data, 1), 'data': {'url': url}}
    return common.walk_tree(node)





