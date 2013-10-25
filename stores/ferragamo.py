# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'ferragamo_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for m1 in re.finditer(ur'<country name="([^"]+)">', body):
        country = cm.html2plain(m1.group(1)).strip()
        sub = cm.extract_closure(body[m1.start():], ur'<country\b', ur'</country>')[0]
        for m2 in re.finditer(ur'<store id="[^"]+">', sub):
            store_sub = cm.extract_closure(sub[m2.start():], ur'<store\b', ur'</store>')[0]
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = country.upper()

            m = re.search(ur'<name>([^<>]+)</name>', store_sub)
            if m is not None:
                entry[cm.name_e] = cm.html2plain(m.group(1)).strip()
            m = re.search(ur'<audience>([^<>]+)</audience>', store_sub)
            if m is not None:
                entry[cm.store_type] = m.group(1).strip()
            m = re.search(ur'<street>([^<>]+)</street>', store_sub)
            if m is not None:
                entry[cm.addr_e] = cm.html2plain(m.group(1)).strip()
            m = re.search(ur'<postal_code>([^<>]+)</postal_code>', store_sub)
            if m is not None:
                entry[cm.zip_code] = m.group(1).strip()
            m = re.search(ur'<city>([^<>]+)</city>', store_sub)
            if m is not None:
                entry[cm.city_e] = cm.html2plain(m.group(1)).strip().upper()
            m = re.search(ur'<phone>([^<>]+)</phone>', store_sub)
            if m is not None:
                tmp = m.group(1).strip()
                if tmp != 'n/a':
                    entry[cm.tel] = tmp
            m = re.search(ur'<fax>([^<>]+)</fax>', store_sub)
            if m is not None:
                tmp = m.group(1).strip()
                if tmp != 'n/a':
                    entry[cm.fax] = tmp
            m = re.search(ur'<email>([^<>]+)</email>', store_sub)
            if m is not None:
                tmp = m.group(1).strip()
                if tmp != 'n/a':
                    entry[cm.email] = tmp
            m = re.search(ur'<lat>(-?\d+\.\d+)</lat>', store_sub)
            if m is not None:
                entry[cm.lat] = string.atof(m.group(1))
            m = re.search(ur'<lon>(-?\d+\.\d+)</lon>', store_sub)
            if m is not None:
                entry[cm.lng] = string.atof(m.group(1))

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
        data = {'url': 'http://www.ferragamo.com/wcsstore/FerragamoStorefrontAssetStore/flash/store',
                'brand_id': 10308, 'brandname_e': u'Salvatore Ferragamo', 'brandname_c': u'菲拉格慕'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results
