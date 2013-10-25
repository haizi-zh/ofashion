# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'xxxx_log.txt'


def fetch_countries(data):
    vals = {'US': 'UNITED STATES', 'GB': 'UNITED KINGDOM', 'CA': 'CANADA'}
    brands = {'US': 'HUSH', 'GB': 'HPUK', 'CA': 'HPCA'}
    results = []
    for item in vals.items():
        d = data.copy()
        d['country_code'], d['country'], d['brand'] = item[0], item[1], brands[item[0]]
        # if d['country_code'] == 'GB':
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    param = {'country': data['country_code'], 'store': data['brand']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for m in re.finditer(ur'GoTo\(\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.lat] = string.atof(m.group(1))
        entry[cm.lng] = string.atof(m.group(2))
        entry[cm.country_e] = data['country']

        sub = cm.extract_closure(body[m.end():], ur'<form\b', ur'</form>')[0]
        m1 = re.search(ur'<a\s+href=', sub)
        if not m1:
            continue
        addr_list = [tmp.strip() for tmp in cm.reformat_addr(sub[:m1.start()]).split(',')]
        pat_tel = re.compile(ur'phone\s*[\.: ]', re.I)
        if re.search(pat_tel, addr_list[-1]):
            entry[cm.tel] = re.sub(pat_tel, '', addr_list[-1])
            del addr_list[-1]
        else:
            tel = cm.extract_tel(addr_list[-1])
            if tel != '':
                entry[cm.tel] = tel
                del addr_list[-1]
        entry[cm.addr_e] = ', '.join(addr_list)

        m2 = re.search(ur'<\s*br\s*>(.+?)<\s*br\s*>', sub[m1.end():])
        type_sub = cm.html2plain(m2.group(1)).strip()
        if 'women' in type_sub.lower() or 'men' in type_sub.lower() \
            or 'kid' in type_sub.lower() or 'child' in type_sub.lower():
            entry[cm.store_type] = type_sub

        entry[cm.city_e] = cm.html2plain(addr_list[-2]).strip().upper()

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

    return tuple(store_list)


def fetch_stores_cn(data):
    vals = ['Storenew%d.html' % idx for idx in xrange(1, 10)]
    vals.append('Store.html')
    store_list = []
    for url in ('http://www.hushpuppies.com.cn/%s' % term for term in vals):
        try:
            body = cm.get_data(url)
        except Exception, e:
            cm.dump('Error in fetching stores: %s' % url, log_name)
            return ()

        city_map = dict((m[0].strip(), m[1].strip()) for m in
                        re.findall(ur'<a rel="([^"]+)" href="#"[^<>]*>([^<>]+)', body))

        start = body.find(ur'<div id="all-list-wrap" style="float:left">')
        if start == -1:
            cm.dump('Error in fetching stores: %s' % url, log_name)
            return ()
        sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

        for m in re.findall(ur'<ul id="([^"]+)"[^<>]*>(.+?)</ul>', sub, re.S):
            city = city_map[m[0].strip()]
            for store in re.findall(ur'<li><a>([^<>]+)', m[1]):
                entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                entry[cm.country_e] = u'CHINA'
                entry[cm.city_e] = city
                entry[cm.addr_e] = store.strip()

                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
                if ret[1] is not None and entry[cm.province_e] == '':
                    entry[cm.province_e] = ret[1]
                if ret[2] is not None and entry[cm.city_e] == '':
                    entry[cm.city_e] = ret[2]
                gs.field_sense(entry)

                cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.country_e],
                                                                    entry[cm.continent_e]), log_name)
                db.insert_record(entry, 'stores')
                store_list.append(entry)
    return store_list


def fetch_stores_au(data):
    url = 'http://www.hushpuppies.com.au/stockists/index/search/'
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    store_list = []
    for s in json.loads(body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = 'AUSTRALIA'

        addr_list = []
        val = s['address']
        if val and cm.html2plain(val).strip() != '':
            addr_list.append(cm.html2plain(val).strip())
        val = s['address1']
        if val and cm.html2plain(val).strip() != '':
            addr_list.append(cm.html2plain(val).strip())
        entry[cm.addr_e] = ', '.join(addr_list)

        val = s['fax']
        entry[cm.fax] = val.strip() if val else ''
        val = s['phone']
        entry[cm.tel] = val.strip() if val else ''
        val = s['postcode']
        entry[cm.zip_code] = val.strip() if val else ''

        val = s['title']
        entry[cm.name_e] = cm.html2plain(val).strip() if val else ''
        val = s['state']
        entry[cm.province_e] = cm.html2plain(val).strip().upper() if val else ''
        val = s['suburb']
        entry[cm.city_e] = cm.html2plain(val).strip().upper() if val else ''

        try:
            val = s['latitude']
            entry[cm.lat] = string.atof(str(val)) if val else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)
        try:
            val = s['longitude']
            entry[cm.lng] = string.atof(str(val)) if val else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)

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

    return tuple(store_list)


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
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url1': 'xxxxxxxxxx',
                'data_url': 'http://dms.wolverineworldwide.com/SearchResults.aspx',
                'brand_id': 10170, 'brandname_e': u'Hush Puppies', 'brandname_c': u'暇步士'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    results.extend(fetch_stores_cn(data))
    results.extend(fetch_stores_au(data))

    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


