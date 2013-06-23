# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'iwc_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    m = re.search(ur'var\s+retailers\s*=\s*', body)
    if m is None:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    end = body.find(u']', m.end())
    if end == -1:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []
    pat = re.compile(ur'[\{,]([a-zA-Z_\d]+):')

    store_list = []
    for s in json.loads(re.sub(re.compile(ur'([\{,])([a-zA-Z_\d]+):'), ur'\1"\2":', body[m.end():end + 1])):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        name_list = []
        for tmp in ['name', 'name_line_2']:
            if tmp in s and s[tmp] is not None and cm.html2plain(s[tmp]).strip() != '':
                name_list.append(cm.html2plain(s[tmp]).strip())
        entry[cm.name_e] = ', '.join(name_list)
        addr_list = []
        for tmp in ['address', 'address_line_2']:
            if tmp in s and s[tmp] is not None and cm.html2plain(s[tmp]).strip() != '':
                addr_list.append(cm.html2plain(s[tmp]).strip())
        entry[cm.addr_e] = ', '.join(addr_list)
        entry[cm.country_e] = s['country'].strip().upper()
        entry[cm.city_e] = s['city'].strip().upper()
        region = cm.html2plain(s['region'])
        if re.search(ur'\d+', region) is None and '&' not in region and ';' not in region:
            entry[cm.province_e] = region.strip().upper()
        entry[cm.zip_code] = s['zip'].strip()
        if s['latitude'].strip() != '':
            entry[cm.lat] = string.atof(s['latitude'])
        if s['longitude'].strip() != '':
            entry[cm.lng] = string.atof(s['longitude'])
        entry[cm.url] = s['url'].strip()
        entry[cm.tel] = s['phone'].strip()
        entry[cm.fax] = s['fax'].strip()
        entry[cm.email] = s['email'].strip()
        store_type = ''
        if s['is_iwc_service']:
            store_type += u'service '
        if s['is_boutique']:
            store_type += u'boutique '
        if s['is_distribution']:
            store_type += u'distribution '
        entry[cm.store_type] = store_type.strip()
        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]

        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)
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
        data = {'url': 'http://www.iwc.com/site_media/CACHE/js/48b43df5a427.js',
                'brand_id': 10176, 'brandname_e': u'IWC', 'brandname_c': u'万国'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

