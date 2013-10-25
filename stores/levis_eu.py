# coding=utf-8
import json
import string
import re
import traceback

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'

db = None
log_name = 'levis_log.txt'
store_map = {}


def gen_city_map():
    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    return json.loads(sub[0])


def fetch_cities(data):
    ret = gs.look_up(data['country_data']['countryID'], 1)
    if ret is None:
        return []

    country = ret['name_e']
    city_map = data['city_map']
    results = []
    if country in city_map:
        for city in city_map[country]:
            d = data.copy()
            d['country'] = country
            d['city'] = city
            d['city_lat'] = city_map[country][city]['lat']
            d['city_lng'] = city_map[country][city]['lng']
            results.append(d)
    return tuple(results)


def fetch_countries(data):
    country_map = {
        'GB': {'country': 'UNITED_KINGDOM_LABEL', 'countryID': 'GB', 'culture': 'en_GB', 'baseUrlGuid': 'GB_GUID',
               'latitude': 54.7, 'longitude': -4.5},
        'PL': {'country': 'POLAND_LABEL', 'countryID': 'PL', 'culture': 'pl_PL', 'baseUrlGuid': 'PL_GUID',
               'latitude': 52, 'longitude': 20},
        'AT': {'country': 'AUSTRIA_LABEL', 'countryID': 'AT', 'culture': 'de_AT', 'baseUrlGuid': 'AT_GUID',
               'latitude': 47.33, 'longitude': 13.33},
        'CH': {'country': 'SWITZERLAND_LABEL', 'countryID': 'CH', 'culture': 'ch_CH', 'baseUrlGuid': 'CH_GUID',
               'latitude': 47, 'longitude': 8},
        'DE': {'country': 'GERMANY_LABEL', 'countryID': 'DE', 'culture': 'de_DE', 'baseUrlGuid': 'DE_GUID',
               'latitude': 51.9, 'longitude': 9},
        'BE': {'country': 'BELGIUM_LABEL', 'countryID': 'BE', 'culture': 'en_BE', 'baseUrlGuid': 'BE_GUID',
               'latitude': 50.83, 'longitude': 4},
        'CZ': {'country': 'CZECH_REPUBLIC_LABEL', 'countryID': 'CZ', 'culture': 'cs_CZ', 'baseUrlGuid': 'CZ_GUID',
               'latitude': 49.75, 'longitude': 15.5},
        'DK': {'country': 'DENMARK_LABEL', 'countryID': 'DK', 'culture': 'da_DK', 'baseUrlGuid': 'DK_GUID',
               'latitude': 56, 'longitude': 10},
        'EU': {'country': 'OTHER_COUNTRIES_LABEL', 'countryID': 'EU', 'culture': 'en_EU',
               'baseUrlGuid': 'otherCountries_GUID', 'latitude': 48.69, 'longitude': 9.14},
        'FI': {'country': 'FINLAND_LABEL', 'countryID': 'FI', 'culture': 'fi_FI', 'baseUrlGuid': 'FI_GUID',
               'latitude': 64, 'longitude': 26},
        'GR': {'country': 'GREECE_LABEL', 'countryID': 'GR', 'culture': 'el_GR', 'baseUrlGuid': 'GR_GUID',
               'latitude': 39, 'longitude': 22},
        'HU': {'country': 'HUNGARY_LABEL', 'countryID': 'HU', 'culture': 'hu_HU', 'baseUrlGuid': 'HU_GUID',
               'latitude': 47, 'longitude': 20},
        'IE': {'country': 'IRELAND_LABEL', 'countryID': 'IE', 'culture': 'en_IE', 'baseUrlGuid': 'IE_GUID',
               'latitude': 50, 'longitude': -8},
        'NL': {'country': 'NETHERLANDS_LABEL', 'countryID': 'NL', 'culture': 'nl_NL', 'baseUrlGuid': 'NL_GUID',
               'latitude': 52.5, 'longitude': 5.75},
        'NO': {'country': 'NORWAY_LABEL', 'countryID': 'NO', 'culture': 'en_NO', 'baseUrlGuid': 'NO_GUID',
               'latitude': 62, 'longitude': 10},
        'PT': {'country': 'PORTUGAL_LABEL', 'countryID': 'PT', 'culture': 'en_PT', 'baseUrlGuid': 'PT_GUID',
               'latitude': 39.3, 'longitude': 8},
        'SE': {'country': 'SWEDEN_LABEL', 'countryID': 'SE', 'culture': 'sv_SE', 'baseUrlGuid': 'SE_GUID',
               'latitude': 62, 'longitude': 15},
        'ES': {'country': 'SPAIN_LABEL', 'countryID': 'ES', 'culture': 'es_ES', 'baseUrlGuid': 'ES_GUID',
               'latitude': 40, 'longitude': -4},
        'FR': {'country': 'FRANCE_LABEL', 'countryID': 'FR', 'culture': 'fr_FR', 'baseUrlGuid': 'FR_GUID',
               'latitude': 46, 'longitude': 2},
        'IT': {'country': 'ITALY_LABEL', 'countryID': 'IT', 'culture': 'it_IT', 'baseUrlGuid': 'IT_GUID',
               'latitude': 42.5, 'longitude': 12.5},
        'RU': {'country': 'RUSSIA_LABEL', 'countryID': 'RU', 'culture': 'ru_RU', 'baseUrlGuid': 'RU_GUID',
               'latitude': 60, 'longitude': 100},
        'TR': {'country': 'TURKEY_LABEL', 'countryID': 'TR', 'culture': 'tr_TR', 'baseUrlGuid': 'TR_GUID',
               'latitude': 39, 'longitude': 35}}
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    results = []
    m = re.search(ur'var \s*BingKey\s*=\s*"([^"]+)"', body)
    if not m:
        return ()
    key = m.group(1)
    for code in country_map:
        val = country_map[code]
        guid = val['baseUrlGuid']
        m = re.search(ur'var \s*%s\s*=\s*"([^"]+)"' % guid, body)
        if not m:
            continue

        if code!='PL':
            continue

        val['baseUrl'] = m.group(1)
        d = data.copy()
        d['key'] = key
        d['country_data'] = val
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = 'https://spatial.virtualearth.net/REST/v1/data/%s/levi_live_%s/LevisBranch?spatialFilter=nearby(%f,%f,400)&$select=__Distance,AddressLine,additionaladdress1,additionaladdress2,PostalCode,Locality,CountryRegion,Latitude,Longitude,BranchName,Phone,levisstoreflagship,levisstorepremium,levisstoreownedoperated,levisstorefranchisee,monday,tuesday,wednesday,thursday,friday,saturday,sunday,stockingwomen,stockingmen,stockingkids,stockingaccessories,belts,hatsscarvesgloves,footwear,eyewear,bags,smallleathergoods,watches,underwear,Levis_Store,Outlet,Authorised_Retailer&$format=json&$top=5&jsonp=GetLocalLevisCallback&key=%s' % (
        data['country_data']['baseUrl'], data['country_data']['culture'], data['city_lat'], data['city_lng'],
        data['key'])
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    store_list = []
    body = re.sub(ur'GetLocalLevisCallback\(', '', body)[:-1]
    for s in json.loads(body)['d']['results']:
        try:
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

            uid = s['__metadata']['uri']
            if uid in store_map:
                cm.dump(u'%s already exists.' % uid, log_name)
                continue

            entry[cm.country_e] = cm.html2plain(s['CountryRegion']).strip().upper()
            entry[cm.native_id] = uid
            entry[cm.city_e] = cm.extract_city(s['Locality'])[0]
            entry[cm.addr_e] = cm.reformat_addr(s['AddressLine'])

            entry[cm.zip_code] = s['PostalCode']
            entry[cm.tel] = s['Phone']
            entry[cm.name_e] = cm.html2plain(s['BranchName']).strip() if s['BranchName'] else ''

            try:
                entry[cm.lat] = string.atof(s['Latitude']) if s['Latitude'] != '' else ''
            except (ValueError, KeyError, TypeError) as e:
                cm.dump('Error in fetching lat: %s' % str(e), log_name)
            try:
                entry[cm.lng] = string.atof(s['Longitude']) if s['Longitude'] != '' else ''
            except (ValueError, KeyError, TypeError) as e:
                cm.dump('Error in fetching lng: %s' % str(e), log_name)

            weekdays = ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')
            hour_list = []
            for day in weekdays:
                if s[day].strip() == '':
                    continue
                hour_list.append('%s: %s' % (day.capitalize(), s[day].strip()))
            entry[cm.hours] = ', '.join(hour_list)

            type_map = {"stockingwomen": "Women", "stockingmen": "Men", "stockingkids": "Kids",
                        "stockingaccessories": "Accessories", "belts": "Belts",
                        "hatsscarvesgloves": "Hats, Scarves, Gloves", "footwear": "Footwear", "eyewear": "Eyewear",
                        "bags": "Bags", "smallleathergoods": "Leather", "watches": "Watches", "underwear": "Underwear"}
            type_list = []
            for key in type_map:
                if s[key] == 'YES':
                    type_list.append(type_map[key])
            entry[cm.store_type] = ', '.join(type_list)

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
            # db.insert_record(entry, 'stores')
            store_list.append(entry)
            store_map[uid] = entry
        except (IndexError, TypeError) as e:
            print traceback.format_exc()
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
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.levi.com/GB/en_GB/findAStore',
                'brand_id': 10215, 'brandname_e': u"Levi's", 'brandname_c': u"Levi's",
                'city_map': gen_city_map()}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


