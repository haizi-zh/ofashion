# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'blancpain_log.txt'


def fetch_contact_info(data, s, store_id):
    url = '%s/%s/detail' % (data['shop_url'], store_id)
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    m=re.search(ur'<div class="contact-info">(.+?)</div>',body,re.S)
    if m is None:
        return s

    entry = s.copy()
    pat_tel=re.compile(ur'\s*Numéro de téléphone\s*[:\.]\s*')
    pat_fax=re.compile(ur'\s*Numéro de fax\s*[:\.]\s*')
    pat_email=re.compile(ur'\s*Adresse électronique\s*[:\.]\s*')
    for term in [tmp.strip() for tmp in cm.reformat_addr(m.group(1)).split(',')]:
        if re.search(pat_tel, term):
            entry[cm.tel]=re.sub(pat_tel,'',term).strip()
        if re.search(pat_fax, term):
            entry[cm.fax]=re.sub(pat_fax,'',term).strip()
        if re.search(pat_email, term):
            entry[cm.email]=re.sub(pat_email,'',term).strip()
    return entry


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    start = body.find(ur'"coords":')
    m = re.search(ur'\[.+?\]', body[start:], re.S)
    store_list = []
    for s in json.loads(m.group()):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        # entry[cm.city_e] = cm.html2plain(s['city']).strip().upper()
        entry[cm.name_e] = cm.html2plain(s['name']).strip()

        try:
            entry[cm.lat] = string.atof(s['latitude'])
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat-lng: %s' % str(e), log_name)
        try:
            entry[cm.lng] = string.atof(s['longitude'])
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat-lng: %s' % str(e), log_name)

        addr_list = []
        for term in (cm.reformat_addr(tmp) for tmp in (s['address1'], s['address2'])):
            if term != '':
                addr_list.append(term)
        addr_list.append(cm.reformat_addr(s['city']))
        entry[cm.addr_e] = ', '.join(addr_list)
        entry[cm.zip_code] = s['postcode'].strip()

        entry = fetch_contact_info(data, entry, s['id'])

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        if entry[cm.country_e] == '' or entry[cm.city_e] == '':
            ret = None
            if entry[cm.lat] != '' and entry[cm.lng] != '':
                ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))
            if ret is None:
                ret = gs.geocode(', '.join((entry[cm.addr_e], s['city'])))

            if ret is not None:
                city = ''
                province = ''
                country = ''
                zip_code = ''
                tmp = ret[0]['address_components']
                for v in tmp:
                    if 'locality' in v['types']:
                        city = v['long_name'].strip().upper()
                    elif 'administrative_area_level_1' in v['types']:
                        province = v['long_name'].strip().upper()
                    elif 'country' in v['types']:
                        country = v['long_name'].strip().upper()
                    elif 'postal_code' in v['types']:
                        zip_code = v['long_name'].strip()
                entry[cm.country_e] = country
                entry[cm.province_e] = province
                entry[cm.city_e] = city
                entry[cm.zip_code] = zip_code

                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e])
                if ret[0] is not None and entry[cm.country_e] == '':
                    entry[cm.country_e] = ret[0]
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
        data = {'url': 'http://www.blancpain.com/en/shop/locator',
                'shop_url': 'http://www.blancpain.com/shop/locator',
                'brand_id': 10046, 'brandname_e': u'Blancpain', 'brandname_c': u'宝珀'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


