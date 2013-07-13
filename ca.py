# coding=utf-8
import json
import string
import re
import traceback
import common as cm
import geosense as gs
from pyquery import PyQuery as pq

__author__ = 'Zephyre'

db = None
log_name = 'ca_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    results = []
    for country in pq(body)('select#country option[value]'):
        d = data.copy()
        if country.attrib['value'] == '0':
            continue
        d['country_code'] = country.attrib['value']
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    param = {'tx_iostorefinder_pi1[__referrer][extensionName]': 'IoStorefinder',
             'tx_iostorefinder_pi1[__referrer][controllerName]': 'Store',
             'tx_iostorefinder_pi1[__referrer][actionName]': 'search',
             'tx_iostorefinder_pi1[countryid]': data['country_code'],
             'tx_iostorefinder_pi1[zip]': 'POSTCODE', 'tx_iostorefinder_pi1[city]': 'Town'}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for s in pq(body)('ul[type="none"] li div.storeDetailUrl'):
        if not s.text:
            continue
        m = re.search(ur'tx_iostorefinder_pi1[^&]+', cm.html2plain(s.text))
        if not m:
            continue

        store = pq(cm.get_data('http://www.c-and-a.com/uk/en/corporate/company/stores/filialen-ajax/?no_cache=1&%s' % m.group()))
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']

        try:
            try:
                entry[cm.city_e] = cm.extract_city(store('td.address span.city')[0].text)[0]
            except (IndexError, TypeError):
                pass
            m = re.search(ur'\[geocode\]\s*=([^,]+),([^,]+)', m.group())
            if m:
                try:
                    entry[cm.lat] = string.atof(m.group(1))
                    entry[cm.lng] = string.atof(m.group(2))
                except ValueError:
                    pass
            try:
                entry[cm.name_e] = cm.html2plain(store('td.address h4')[0].text).strip()
            except (IndexError, TypeError):
                pass
            try:
                tmp = store('td.address span.zip')[0].text
                entry[cm.zip_code] = tmp if tmp else ''
            except IndexError:
                pass
            try:
                entry[cm.addr_e] = '%s, %s %s' % (
                    cm.html2plain(store('td.address span.street')[0].text).strip(), entry[cm.city_e],
                    entry[cm.zip_code])
            except (IndexError, TypeError):
                pass
            try:
                tmp = store('td.address span.tel')[0].text
                entry[cm.tel] = tmp if tmp else ''
            except IndexError:
                pass
            try:
                tmp = store('td.address span.fax')[0].text
                entry[cm.fax] = tmp if tmp else ''
            except IndexError:
                pass

            hours_list = []
            for item in (cm.reformat_addr(unicode(pq(tmp))) for tmp in store('td.opening table tr')):
                if 'opening times' in item.lower():
                    continue
                hours_list.append(re.sub(ur':\s*,\s*', ': ', item))
            entry[cm.hours] = ', '.join(hours_list)

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

        except (IndexError, TypeError) as e:
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
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {
            'data_url': 'http://www.c-and-a.com/uk/en/corporate/company/stores/storefinder/?no_cache=1&tx_iostorefinder_pi1%5Baction%5D=search&tx_iostorefinder_pi1%5Bcontroller%5D=store',
            'url': 'http://www.c-and-a.com/uk/en/corporate/fashion/stores/',
            'brand_id': 10059, 'brandname_e': u'C&A', 'brandname_c': u'C&A'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


