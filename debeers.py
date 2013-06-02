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
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    host = 'http://www.debeers.com.cn'
    if type == 0:
        url = host + '/stores'
    print('Fetching for %s' % url)
    response = opener.open(url)
    html = response.read()

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

        m = re.findall(r'<h2 class="store-name">(.+?)</h2>', html, re.U)
        if m is not None:
            store['name'] = m[0].strip()

        start = html.find('<h3>营业时间</h3>')
        if not start == -1:
            start += '<h3>营业时间</h3>'.__len__()
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

            start = addr_src.find('<h3>地址</h3>')
            if not start == -1:
                start += '<h3>地址</h3>'.__len__()
                end = addr_src.find('<p', start)
                addr_src = addr_src[start:end].strip()
                addr_entries = addr_src.split('<br />')
                addr_entries = [val.strip() for val in addr_entries]
                store['addr'] = '\r\n'.join(addr_entries)

        print('Found store: %s, %s, %s' % (store['name'], store['tel'], opt['continent']))
        return [store]


def fetch():
    stores = get_stores(None, 0, None)
    return stores