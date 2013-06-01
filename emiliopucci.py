# coding=utf-8

__author__ = 'Zephyre'

import json
import string
import urllib
import urllib2
import re
import common


def get_countries():
    """
    格式：
    {'Asia':[{'countri_id':884,'country':'brazil'}]}
    """
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    url = 'http://home.emiliopucci.com/boutiques'
    response = opener.open(url)
    html = response.read()

    # 开始解析
    start = html.find('<select name="country_id" id="country_id">')
    if start == -1:
        return []
    end = html.find('</select>', start)
    html = html[start:end]

    districts = {}
    start = 0
    while True:
        # 获得洲信息
        start = html.find('<optgroup', start)
        if start == -1:
            break
        end = html.find('</optgroup>', start) + '</optgroup>'.__len__()
        con = html[start:end]
        start = end

        m = re.findall(r'<optgroup label="([\w\s]+)">', con, re.S)
        if m is None:
            continue
        continent = m[0]

        itor = re.finditer(r'<option value="(\d+)">([\w\s]+)</option>', con, re.S)
        countries = []
        for m in itor:
            country_id = string.atoi(m.group(1))
            country = m.group(2)
            countries.append({'country': country, 'country_id': country_id})
        districts[continent] = countries

    return districts


def fetch_stores(continent, country, cid):
    """
    cid: country_id
    """
    url = 'http://home.emiliopucci.com/boutiques'
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    response = opener.open(url, 'country_id=%d' % cid)
    html = response.read()
    start = html.find('class="boutique_store"')
    if start == -1:
        return []
    end = html.find('</ul>', start)
    html = html[start:end]

    # <li><h6>Paris</h6><p>36 Avenue Montaigne<br />+33 1 47 20 04 45<br />France</p></li>
    stores = []
    for m in re.finditer(r'<li><h6>([\w\s]+)</h6><p>(.*?)</p></li>', html):
        city = m.group(1)
        content = m.group(2) + r'<br />'
        addr = ''
        store_item = {}
        for m1 in re.finditer(r'(.*?)<br\s*?/>', content):
            # 是否为电话？
            m_tel = re.match('[\d+ ]+', m1.group(1))
            if m_tel is not None:
                store_item['tel'] = m1.group(1)
            else:
                addr += m1.group(1) + '\r\n'
        store_item['address'] = addr.strip()
        store_item['country'] = country['country']
        store_item['continent'] = continent
        store_item['city'] = city
        stores.append(store_item)
    return stores


def fetch():
    entries = get_countries()
    stores = []
    for con in entries.keys():
        print('Fetching for %s...' % con)
        for c in entries[con]:
            print('Fetching for %s...' % c['country'])
            col = fetch_stores(con, c, c['country_id'])
            if col is not None:
                stores.extend(col)
                for s in col:
                    print(s)
