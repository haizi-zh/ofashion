# coding=utf-8
import json
import string
import re
import traceback
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'longchamp_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    m1 = re.search(ur'var\s+markers\s*=\s*\[', body)
    if not m1:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()
    body = body[m1.end() - 1:]
    m2 = re.search(ur'\]\s*;', body)
    if not m2:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()
    raw = json.loads(body[:m2.end() - 1])

    store_list = []
    for s in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        try:
            try:
                entry[cm.lat] = string.atof(str(s['location'][0]))
                entry[cm.lng] = string.atof(str(s['location'][1]))
            except (KeyError, IndexError, ValueError, TypeError):
                pass

            s = s['content']
            try:
                entry[cm.name_e] = cm.html2plain(s['title']).strip()
            except (KeyError, TypeError):
                pass

            tmp_list = s['analytics_label'].split('-')
            entry[cm.country_e] = tmp_list[0]
            entry[cm.city_e] = cm.extract_city(tmp_list[1])[0]

            try:
                entry[cm.addr_e] = cm.reformat_addr(s['address']).strip()
            except (KeyError, TypeError):
                pass

            try:
                entry[cm.fax] = s['fax'].strip()
            except (KeyError, TypeError):
                pass
            try:
                entry[cm.tel] = s['phone'].strip()
            except (KeyError, TypeError):
                pass
            try:
                entry[cm.email] = s['mail'].strip()
            except (KeyError, TypeError):
                pass
            try:
                entry[cm.url] = u'http://en.longchamp.com/store/map' + s['url'].strip()
            except (KeyError, TypeError):
                pass
            try:
                entry[cm.zip_code] = cm.html2plain(s['zipcode_town']).replace(tmp_list[1], '').strip()
            except (KeyError, TypeError):
                pass

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)

            cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.city_e],
                                                                    entry[cm.country_e], entry[cm.continent_e]),
                    log_name)
            db.insert_record(entry, 'stores')
            store_list.append(entry)

        except Exception as e:
            cm.dump(traceback.format_exc(), log_name)
            continue

    return tuple(store_list)


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
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://en.longchamp.com/store/map',
                'brand_id': 10510, 'brandname_e': u'Longchamp', 'brandname_c': u'Longchamp'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


