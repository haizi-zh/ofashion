# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'patek_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    start = body.find(ur'<select id="ddl_country_all"')
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    country_map = {}
    for m in re.findall(ur'<option value="\d+;(\d+)">([^<>]+)</option>',
                        cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]):
        country_map[string.atoi(m[0])] = cm.html2plain(m[1]).strip().upper()

    start = body.find(ur'<select id="ddl_state"')
    if start == -1:
        cm.dump('Error in fetching states: %s' % url, log_name)
        return []
    state_map = {}
    for m in re.findall(ur'<option value="(\d+)">([^<>]+)</option>',
                        cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]):
        state_map[string.atoi(m[0])] = m[1].strip().upper()

    start = body.find(ur'<select id="ddl_city_all"')
    if start == -1:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []
    city_map = {}
    for m in re.findall(ur'<option value="(\d+);[^"]+">([^<>]+)</option>',
                        cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]):
        city_map[string.atoi(m[0])] = cm.html2plain(m[1]).strip().upper()

    start = body.find(ur'<div id="content_specific"')
    if start == -1:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []
    store_sub = cm.extract_closure(body[start:], ur'<script>', ur'</script')[0]
    store_list = []
    store_id = 0
    while True:
        m = re.search(ur'datas\[%d\]' % store_id, store_sub)
        if m is None:
            break

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        m = re.search(ur'datas\[%d\]\.Name\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            entry[cm.name_e] = cm.html2plain(m.group(1)).decode('unicode_escape').strip()

        m = re.search(ur'datas\[%d\]\.Address\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            entry[cm.addr_e] = cm.html2plain(m.group(1)).decode('unicode_escape').strip()

        m = re.search(ur'datas\[%d\]\.PostCode\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            entry[cm.zip_code] = cm.html2plain(m.group(1)).strip()

        m = re.search(ur'datas\[%d\]\.City\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            entry[cm.city_e] = cm.extract_city(m.group(1))[0]

        m = re.search(ur'datas\[%d\]\.Country\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            entry[cm.country_e] = cm.html2plain(m.group(1)).strip().upper()

        m = re.search(ur'datas\[%d\]\.UsaStateId\s*=\s*([^;]+);' % store_id, store_sub)
        if m is not None:
            tmp = string.atoi(m.group(1))
            if tmp != -1:
                entry[cm.province_e] = state_map[tmp]

        m = re.search(ur'datas\[%d\]\.Telephone\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            entry[cm.tel] = cm.html2plain(m.group(1)).strip()

        m = re.search(ur'datas\[%d\]\.Fax\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            entry[cm.fax] = cm.html2plain(m.group(1)).strip()

        m = re.search(ur'datas\[%d\]\.Email\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            entry[cm.email] = cm.html2plain(m.group(1)).strip()

        m = re.search(ur'datas\[%d\]\.Latitude\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            tmp = string.atoi(m.group(1))
            if tmp != -1:
                entry[cm.lat] = string.atof(tmp)

        m = re.search(ur'datas\[%d\]\.Longitude\s*=\s*"([^"]+)"' % store_id, store_sub)
        if m is not None:
            tmp = string.atoi(m.group(1))
            if tmp != -1:
                entry[cm.lng] = string.atof(tmp)

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)
        store_id += 1

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.patek.com/contents/default/en/findretailer.html',
                'brand_id': 10296, 'brandname_e': u'Patek Philippe', 'brandname_c': u'百达翡丽'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results