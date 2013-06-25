# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'montblanc_log.txt'


def fetch_countries(data):
    results = []
    vars = {55: 'Andorra', 60: 'Argentina', 63: 'Australia', 64: 'Austria', 65: 'Azerbaijan', 67: 'Bahrain',
            71: 'Bslgium', 80: 'Brazil', 83: 'Bulgaria', 88: 'Canada', 90: 'Cayman Islands', 93: 'Chile',
            94: 'China',
            97: 'Colombia', 104: 'Cyprus', 105: 'Czech Republic', 106: 'Denmark', 110: 'Ecuador', 111: 'Egypt',
            115: 'Estonia', 121: 'France', 551: 'Germany', 554: 'Greece', 559: 'Guatemala', 566: 'Hong Kang',
            567: 'Hungary', 124: 'India', 125: 'Indonesia', 129: 'Israel', 130: 'Italy', 132: 'Japan',
            133: 'Jordan',
            134: 'Kasachtan', 138: 'Kuwait', 140: 'Latvia', 141: 'Lebanon', 147: 'Luxembourg', 148: 'Macau',
            152: 'Malaysia', 155: 'Malta', 161: 'Mexico', 164: 'Monaco', 165: 'Mongolia', 167: 'Morocco',
            173: 'Netherlands', 174: 'Netherlands Antilles', 176: 'New Zealand', 183: 'Norway', 185: 'Pakistan',
            187: 'Panama', 189: 'Paraguay', 190: 'Peru', 191: 'Philippines', 193: 'Poland', 194: 'Portugal',
            195: 'Puerto Rico', 196: 'Qatar', 198: 'Romania', 199: 'Russian Federation', 209: 'Saudi Arabia',
            213: 'Singapore', 218: 'South Africa', 1284: 'South Korea', 219: 'Spain', 1799: 'Sultanate of Oman',
            222: 'Surname', 225: 'Sweden', 226: 'Switzerland', 907: 'Taiwan', 231: 'Thailand', 237: 'Turkey',
            242: 'Ukraine', 243: 'United Arab Emirates', 244: 'United Kingdom', 245: 'United States of America',
            247: 'Uruguay', 248: 'Uzbekistan', 251: 'Venezuela', 1633: 'Vietnam'}
    for key in vars:
        d = data.copy()
        d['country_id'] = key
        d['country'] = vars[key]
        # if d['country_id'] == 194:
        results.append(d)
    return results


def fetch_stores(data):
    url = data['data_url']
    param = {'cou': data['country_id'], 'f': 'bran'}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    for s in json.loads(body)['branches']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = cm.html2plain(s['country']['name']).strip().upper()

        addr_list = []
        for term in (cm.html2plain(s[key]).strip() for key in ('adress_%d' % idx for idx in xrange(1, 4))):
            if term is not None and term != '':
                addr_list.append(term)
        entry[cm.addr_e] = ', '.join(addr_list)

        assortment_list = []
        for term in (tmp['name'] for tmp in s['assortment']):
            if term is not None and term.strip() != '':
                assortment_list.append(term.strip())
        entry[cm.store_type] = ', '.join(assortment_list)

        if s['city']['name'] is not None:
            entry[cm.city_e] = cm.html2plain(s['city']['name']).strip().upper()
        if s['email'] is not None:
            entry[cm.email] = s['email'].strip()
        if s['fax'] is not None:
            entry[cm.fax] = s['fax'].strip()
        if s['fulladress'] is not None and cm.html2plain(s['fulladress']).strip() != '':
            entry[cm.addr_e] = re.sub(ur'[,\s]+$', '', cm.html2plain(s['fulladress'])).strip()
        if 'latitude' in s and s['latitude'] is not None and s['latitude'].strip() != '':
            try:
                entry[cm.lat] = string.atof(s['latitude'])
            except ValueError:
                pass
        if 'longitude' in s and s['longitude'] is not None and s['longitude'].strip() != '':
            try:
                entry[cm.lng] = string.atof(s['longitude'])
            except ValueError:
                pass
        entry[cm.name_e] = s['name'].strip()
        entry[cm.tel] = s['phone'].strip()

        hour_list = []
        for term in (s[key] for key in ('time_open_%d' % idx for idx in xrange(1, 4))):
            if term is not None and term.strip() != '':
                hour_list.append(term.strip())
        entry[cm.hours] = ', '.join(hour_list)

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
        data = {'data_url': 'http://stores.montblanc.com/client.json',
                'url': 'http://www.gucci.com/us/storelocator',
                'brand_id': 10266, 'brandname_e': u'MONTBLANC', 'brandname_c': u'万宝龙'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

