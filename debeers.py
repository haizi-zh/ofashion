# coding=utf-8

__author__ = 'Zephyre'

import json
import string
import urllib
import urllib2
import re
import common


def get_stores(url, type, opt):
    """
    获得洲，城市等信息
    """
    host = 'http://www.debeers.com.cn'
    if type == 0:
        url = host + '/stores'
    html = common.get_data(url)

    if type == 0:
        # 洲列表
        pat_s = '<ul class="tab-link-set">'
        pat_e = '</ul>'
        pat_entry = r'<a class="tab-link-a" href="/stores/(\w+)" title="([^\s]+?)">'
        entries = []

        start = html.find(pat_s)
        if start == -1:
            return []
        end = html.find(pat_e, start) + pat_e.__len__()
        html = html[start:end]

        for m in re.finditer(pat_entry, html, re.U):
            c_code = m.group(1)
            c_name = m.group(2)
            c_url = url + '/%s' % c_code
            entries.append({'type': 0, 'code': c_code, 'name': c_name, 'url': c_url})

        stores = []
        for e in entries:
            stores.extend(get_stores(e['url'], 1, {'continent': e['name']}))
        return stores

    elif type == 1:
        # 店铺列表
        pat_s = '<ul class="store-link-set">'
        pat_e = '</ul>'
        pat_entry = r'<a href="([^\s]+?)" title="(.+?)" class="store-link-a">'

        entries = []
        start = html.find(pat_s)
        if start == -1:
            return []
        end = html.find(pat_e, start) + pat_e.__len__()
        html = html[start:end]

        for m in re.finditer(pat_entry, html, re.U):
            c_url = host + m.group(1)
            c_name = m.group(2)
            entries.append({'type': 1, 'name': c_name, 'url': c_url})

        stores = []
        for e in entries:
            opt['url'] = e['url']
            stores.extend(get_stores(e['url'], 2, opt))
        return stores
    elif type == 2:
        # 店铺信息
        pat_s = '<div class="store-details">'
        pat_e = '<div class="share">'
        start = html.find(pat_s)
        if start == -1:
            return []
        end = html.find(pat_e, start)
        html = html[start:end]

        store = {'type': 2}
        store['url'] = opt['url']
        store['continent'] = opt['continent']

        m = re.findall(r'<h2 class="store-name">(.+?)</h2>', html, re.U)
        if m is not None:
            store['name'] = m[0].strip()

        start = html.find(u'<h3>营业时间</h3>')
        if not start == -1:
            start += u'<h3>营业时间</h3>'.__len__()
            end = html.find('</div>', start)
            hour_str = html[start:end].strip()
            store['hours'] = hour_str

        start = html.find('<div class="store-address">')
        if not start == -1:
            end = html.find('</div>', start)
            addr_src = html[start:end].strip()

            m = re.findall(r'<p class="store-phone">\s*(.*?)\s*</p>', addr_src, re.S)
            if m is not None:
                store['tel'] = m[0]
            m = re.findall(r'<p class="store-fax">\s*(.*?)\s*</p>', addr_src, re.S)
            if m is not None:
                store['fax'] = m[0]
            m = re.findall(r'<p class="store-email">\s*(.*?)\s*</p>', addr_src, re.S)
            if m is not None:
                store['email'] = m[0]

            start = addr_src.find(u'<h3>地址</h3>')
            if not start == -1:
                start += u'<h3>地址</h3>'.__len__()
                end = addr_src.find('<p', start)
                addr_src = common.reformat_addr(addr_src[start:end])
                store['addr'] = addr_src

        print('Found store: %s, %s, %s, (%s)' % (store['name'], store['addr'], store['tel'], store['continent']))
        return [store]


def fetch():
    stores = get_stores(None, 0, None)
    return stores