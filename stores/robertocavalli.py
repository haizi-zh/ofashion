# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'robertocavalli_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    start = body.find(ur'<label>COUNTRY *</label>')
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    sub = cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]
    results = []
    for m in re.findall(ur'<option value="(\d+)"[^<>]*>([^<>]+)</option>', sub):
        d = data.copy()
        d['country_id'] = string.atoi(m[0])
        d['country'] = m[1].strip().upper()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['data_url']
    param = {'n': data['country_id']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    for s in json.loads(body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = s['citta'].strip().upper()

        entry[cm.addr_e] = cm.reformat_addr(s['indirizzo'])
        entry[cm.tel] = s['telefono'].strip()
        entry[cm.fax] = s['fax'].strip()
        entry[cm.email] = s['eMail'].strip()
        entry[cm.zip_code] = s['cap'].strip()
        entry[cm.store_type] = ', '.join([item['name'] for item in s['brandTaxNodeFEList']])
        entry[cm.store_class] = s['tipologiaTaxNodeFE']['name']

        m = re.findall(ur'-?\d+\.\d+', s['posizioneGoogleMap'])
        if len(m) == 2:
            entry[cm.lat] = string.atof(m[0])
            entry[cm.lng] = string.atof(m[1])

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        gs.field_sense(entry)
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
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.robertocavalli.com/mobile/storeLocatorGetNegozi.plp',
                'url': 'http://www.robertocavalli.com/store_locator/',
                'brand_id': 10305, 'brandname_e': u'Roberto Cavalli', 'brandname_c': u'罗伯特·卡沃利'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

