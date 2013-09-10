# coding=utf-8
import _mysql
import logging
import logging.config
from math import cos, sin, acos
import hashlib
import re
import time
import sys
import common as cm
from pyquery import PyQuery as pq
import geosense

__author__ = 'Zephyre'
# Fetch the geocode information for every store record, based on its address and location.

logging.config.fileConfig('geocode_fetch.cfg')
logger = logging.getLogger('firenzeLogger')


def update_geo_shift(db, id_range=None, overwrite=False, extra_condition=None, block_size=500):
    def calc_distance(p1, p2):
        """
        Calculate the distance between two coordinates in kilometers
        :param p1:
        :param p2:
        """
        lat1, lng1, lat2, lng2 = tuple(v / 180.0 * 3.14 for v in tuple(p1) + tuple(p2))
        R = 6373
        v = cos(lat1) * cos(lat2) * (cos(lng1) * cos(lng2) + sin(lng1) * sin(lng2)) + sin(lat1) * sin(lat2)
        return R * acos(v if v <= 1.0 else 1.0)

    cond = 'WHERE lat IS NOT NULL AND lng IS NOT NULL AND geo_location_lat IS NOT ' \
           'NULL AND geo_location_lng IS NOT NULL AND flag=1'
    if not extra_condition:
        extra_condition = []
    if not overwrite:
        extra_condition.append('geo_shift IS NULL')
    if id_range:
        extra_condition.append(str.format(' idstores>={0} AND idstores<{1}', *id_range))
    if len(extra_condition) > 0:
        cond += str.format(' AND {0}', ' AND '.join(extra_condition))
    sc = str.format('SELECT COUNT(*) FROM stores {0}', cond)
    db.query(sc)
    total_cnt = int(db.store_result().fetch_row()[0][0])
    idx = 0
    max_idstores = 0
    logger.info(unicode.format(u'update_geo_shift: {0} records fetched', total_cnt))

    while True:
        temp_cond = ' AND '.join((cond, str.format('idstores>={0}', max_idstores + 1)))
        statement = str.format('SELECT idstores, lat, lng, geo_location_lat, geo_location_lng '
                               'FROM stores {0} LIMIT {1}', temp_cond, block_size)
        db.query(statement)
        record_set = db.store_result()
        if record_set.num_rows() == 0:
            break

        while True:
            data = record_set.fetch_row(how=1)
            if len(data) == 0:
                break
            else:
                data = data[0]
            idx += 1

            idstores = int(data['idstores'])
            lat, lng, g_lat, g_lng = map(lambda key: float(data[key]) if data[key] else None,
                                         ['lat', 'lng', 'geo_location_lat', 'geo_location_lng'])
            if not (lat and lng and g_lat and g_lng):
                continue
            distance = calc_distance([lat, lng], [g_lat, g_lng])
            statement = str.format('UPDATE stores SET geo_shift={0} WHERE idstores={1}', distance, idstores)
            db.query(statement)
            logger.info(unicode.format(u'Updated {0}/{1} ({2:.2%}): idstores={3}, geo_shift={4}', idx,
                                       total_cnt, float(idx) / total_cnt, idstores, distance))
            max_idstores = idstores

    logger.info(u'Done')


def results_stats(terms_list):
    """
    Return the numbers of the results returned by a google query.
    :param terms_list: a list of search terms
    """

    def parse_search_result(query):
        # query = str.format('"{0}"', re.sub('\s+', '+', query.encode('utf-8')))
        query = str.format('"{0}"', query.encode('utf-8'))
        # body = cm.get_data('https://www.google.com.hk/search', data={'q': query})
        body = cm.get_data('http://www.bing.com/search', data={'q': query})
        # time.sleep(0.5)
        # temp = pq(body)('#resultStats')
        temp = pq(body)('span#count')
        if len(temp) > 0:
            v = temp[0].text
            if v:
                v = re.sub(ur'\s*,\s*', u'', v)
                m = re.search(ur'(\d+)', v)
                return int(m.group(1)) if m else 0
            else:
                return 0
        else:
            return 0

    return tuple(map(parse_search_result, terms_list))


def get_idstores_list(db, skip_existing=True, id_range=None, extra_condition=None):
    """
    Get the list of all idstores.
    :param db:
    :param excluded: ids of excluded brands, iterable.
    :param skip_existing: skip records which already have geocode data?
    :param id_range:
    """
    if not extra_condition:
        extra_condition = []
    statement = 'SELECT idstores FROM stores'
    cond_list = ['flag=1']
    if skip_existing:
        cond_list.append('geo_queried="N/A"')
    if id_range:
        cond_list.append(str.format('idstores>={0} AND idstores<{1}', id_range[0], id_range[1]))
    if extra_condition:
        cond_list += extra_condition
    if cond_list:
        cond = str.format(' WHERE {0}', ' AND '.join(cond_list))
    else:
        cond = ''
    statement += cond + ' ORDER BY idstores'

    db.query(statement)
    recordset = db.store_result()
    result = recordset.fetch_row(maxrows=0)
    store_list = tuple(int(row[0]) for row in result)
    logger.info(str.format('{0} records fetched: {1}...', len(store_list),
                           ', '.join((str(brand_id) for brand_id in store_list[:10]))))
    return store_list


def geocode_query(db, id_range=None, extra_condition=None):
    """
    Perform the geocode_query with the address data.
    :param db:
    :param id_range:
    :raise:
    """

    def gen_search_list(result):
        """
        Generate search terms in the following formats: addr,region,country
        :param result:
        """
        country = result['country_e']
        if not country or not country.strip():
            country = u''
        else:
            country = country.decode('utf-8').strip()

        province = result['province_e']
        if not province or not province.strip():
            province = u''
        else:
            province = province.decode('utf-8').strip()

        city = result['city_e']
        if not city or not city.strip():
            city = u''
        else:
            city = city.decode('utf-8').strip()

        if result['addr_e'] and result['addr_e'].strip():
            addr = result['addr_e'].decode('utf-8').strip()
        elif result['addr_c'] and result['addr_c'].strip():
            addr = result['addr_c'].decode('utf-8').strip()
        elif result['addr_l'] and result['addr_l'].strip():
            addr = result['addr_l'].decode('utf-8').strip()
        else:
            logger.error(unicode.format(u'Address missing for {0}: {1}', idstore, result))
            db.query(unicode.format(u'UPDATE stores SET geo_queried="FAIL" WHERE idstores={0}',
                                    idstore).encode('utf-8'))
            return ()
            # Remove postal codes from the address
        addr = re.sub(ur'\d{4,}', u'', addr)
        return (addr, ','.join((addr, city, country)))


    # The following brands are excluded from the fetch.
    # excluded_brand = {10004, 10095, 10123, 10170, 10196, 10227, 10277, 10279, 10318, 10359, 10361, 10363, 10017, 10309,
    #                   10371}

    # Overwrite existing geocode data?
    overwrite = False
    idstores_list = get_idstores_list(db, id_range=id_range, extra_condition=extra_condition)
    total_cnt = len(idstores_list)
    for i in xrange(total_cnt):
        idstore = idstores_list[i]
        db.query(str.format(
            'SELECT * FROM stores WHERE idstores={0}',
            idstore))
        record_set = db.store_result()
        result = record_set.fetch_row(how=1, maxrows=1)[0]

        geo_result = None
        search_term = u''
        for search_term in gen_search_list(result):
            geo_result = geosense.geocode(addr=search_term, retry=5, logger=logger)
            if geo_result and len(geo_result) >= 1:
                break
        if not geo_result:
            logger.error(unicode.format(u'No geocode result returned for {0} / address={1}', idstore, search_term))
            db.query(unicode.format(u'UPDATE stores SET geo_queried="FAIL" WHERE idstores={0}',
                                    idstore).encode('utf-8'))
            continue
        geo_result = geo_result[0]
        geo_dict = {}

        # Make sure all the keys in geo_result are registered
        if not (set(geo_result.keys()) - {'administrative_info'} <= {'address_components', 'formatted_address',
                                                                     'geometry', 'partial_match', 'types',
                                                                     'postcode_localities'}):
            raise LookupError(unicode.format(u'Unknown fields {0} for {1} / address={2}', set(geo_result.keys()),
                                             idstore, search_term))

        # Parse the geocode result which is in JSON format.
        if 'address_components' not in geo_result:
            logger.error(unicode.format(u'No address_components found for {0} / address={1}', idstore, search_term))
            db.query(unicode.format(u'UPDATE stores SET geo_queried="FAIL" WHERE idstores={0}',
                                    idstore).encode('utf-8'))
            continue
        for item in geo_result['address_components']:
            # type = ['political']
            if len(item['types']) == 1 and item['types'][0] == 'political':
                continue
            elif len(item['types']) == 0:
                continue

            # Make sure all the keys in address_components are registered
            if len(set(item['types']) & {'street_number', 'route', 'locality', 'administrative_area_level_1',
                                         'administrative_area_level_2', 'administrative_area_level_3',
                                         'country', 'postal_code', 'point_of_interest', 'sublocality',
                                         'premise', 'subpremise', 'establishment', 'neighborhood',
                                         'postal_town', 'colloquial_area', 'post_box', 'street_address'}) == 0:
                raise LookupError(unicode.format(u'Unknown type: {0} for {1} / address={2}', item['types'],
                                                 idstore, search_term))
            if 'street_number' in item['types']:
                geo_dict['geo_street_number'], geo_dict['geo_street_number_short'] = item['long_name'], item[
                    'short_name']
                continue
            elif 'route' in item['types']:
                geo_dict['geo_route'], geo_dict['geo_route_short'] = item['long_name'], item['short_name']
                continue
            elif 'establishment' in item['types']:
                geo_dict['geo_establishment'], geo_dict['geo_establishment_short'] = item['long_name'], item[
                    'short_name']
                continue
            elif 'neighborhood' in item['types']:
                geo_dict['geo_neighborhood'], geo_dict['geo_neighborhood_short'] = item['long_name'], item[
                    'short_name']
                continue
            elif 'postal_town' in item['types']:
                geo_dict['geo_postal_town'], geo_dict['geo_postal_town_short'] = item['long_name'], item[
                    'short_name']
                continue
            elif 'colloquial_area' in item['types']:
                geo_dict['geo_colloquial_area'], geo_dict['geo_colloquial_area_short'] = item['long_name'], item[
                    'short_name']
                continue
            elif 'sublocality' in item['types']:
                geo_dict['geo_sublocality'], geo_dict['geo_sublocality_short'] = item['long_name'], item[
                    'short_name']
                continue
            elif 'locality' in item['types']:
                geo_dict['geo_locality'], geo_dict['geo_locality_short'] = item['long_name'], item['short_name']
                continue
            elif 'administrative_area_level_1' in item['types']:
                geo_dict['geo_administrative_area_level_1'], geo_dict['geo_administrative_area_level_1_short'] = \
                    item['long_name'], item['short_name']
                continue
            elif 'administrative_area_level_2' in item['types']:
                geo_dict['geo_administrative_area_level_2'], geo_dict['geo_administrative_area_level_2_short'] = \
                    item['long_name'], item['short_name']
                continue
            elif 'administrative_area_level_3' in item['types']:
                geo_dict['geo_administrative_area_level_3'], geo_dict['geo_administrative_area_level_3_short'] = \
                    item['long_name'], item['short_name']
                continue
            elif 'country' in item['types']:
                geo_dict['geo_country'], geo_dict['geo_country_short'] = item['long_name'], item['short_name']
                continue
            elif 'postal_code' in item['types']:
                geo_dict['geo_postal_code'], geo_dict['geo_postal_code_short'] = item['long_name'], \
                                                                                 item['short_name']
                continue

        if 'geometry' not in geo_result:
            logger.error(unicode.format(u'No geometry found for {0} / address={1}', idstore, search_term))
            db.query(unicode.format(u'UPDATE stores SET geo_queried="FAIL" WHERE idstores={0}',
                                    idstore).encode('utf-8'))
            continue
        geometry = geo_result['geometry']
        if 'location' not in geometry:
            logger.error(unicode.format(u'No location data found for {0} / address={1}', idstore, search_term))
            db.query(unicode.format(u'UPDATE stores SET geo_queried="FAIL" WHERE idstores={0}',
                                    idstore).encode('utf-8'))
            continue

        # Make sure all keys in geometry are registered
        if not (set(geometry.keys()) <= {'bounds', 'viewport', 'location_type', 'location'}):
            raise LookupError(unicode.format(u'Unknown fields {0} for {1} / address={2}', geometry.keys(),
                                             idstore, search_term))
        geo_dict['geo_location_lat'] = float(geometry['location']['lat'])
        geo_dict['geo_location_lng'] = float(geometry['location']['lng'])
        if 'bounds' in geometry:
            geo_bounds = geo_result['geometry']['bounds']
            geo_dict['geo_bounds_ne_lat'] = float(geo_bounds['northeast']['lat'])
            geo_dict['geo_bounds_ne_lng'] = float(geo_bounds['northeast']['lng'])
            geo_dict['geo_bounds_sw_lat'] = float(geo_bounds['northeast']['lat'])
            geo_dict['geo_bounds_sw_lng'] = float(geo_bounds['northeast']['lng'])
        if 'viewport' in geometry:
            geo_viewport = geometry['viewport']
            geo_dict['geo_viewport_ne_lat'] = float(geo_viewport['northeast']['lat'])
            geo_dict['geo_viewport_ne_lng'] = float(geo_viewport['northeast']['lng'])
            geo_dict['geo_viewport_sw_lat'] = float(geo_viewport['northeast']['lat'])
            geo_dict['geo_viewport_sw_lng'] = float(geo_viewport['northeast']['lng'])
        if 'location_type' in geometry:
            geo_dict['geo_location_type'] = geometry['location_type']

        if 'formatted_address' in geo_result:
            geo_dict['geo_formatted_address'] = geo_result['formatted_address']
        if 'partial_match' in geo_result:
            geo_dict['geo_partial_match'] = geo_result['partial_match']
        if 'types' in geo_result:
            geo_dict['geo_types'] = geo_result['types']
        if 'postcode_localities' in geo_result:
            geo_dict['geo_postcode_localities'] = geo_result['postcode_localities']
        geo_dict['geo_query_param'] = search_term
        geo_dict['geo_queried'] = u'PASS'

        term_list = []
        for k in geo_dict:
            v = geo_dict[k]
            if isinstance(v, float) or isinstance(v, int):
                term_list.append(unicode.format(u'{0}={1}', k, v))
            else:
                v = re.sub(ur'(?<!\\)"', ur'\\"', unicode(v))
                term_list.append(unicode.format(u'{0}="{1}"', k, v))

        # FIXME If certain keys are not present in term_list, e.g. geo_locality, the generated update_str
        # will leave corresponding fields alone instead of modifying them. If these fields happen to have old values,
        # they will be kept untouched.
        update_str = unicode.format(u'UPDATE stores SET ') + u', '.join(term_list) + \
                     unicode.format(u' WHERE idstores={0}', idstore)
        db.query(update_str.encode('utf-8'))

        logger.info(unicode.format(u'{0}/{1} completed({2:.2%}): idstores={3}', i, total_cnt,
                                   float(i) / total_cnt, idstore))

    logger.info('Done.')

# TODO Create a function to reset geocoded records: reset idcity and geo_queried, delete corresponding
# records from city and city_fingerprints.

def insert_new_city(db, result, ratio_threshold=10, big_cities=None):
    city_info = {u'country': result[u'geo_country'].upper(), u'code2': result[u'geo_country_short'].upper()}
    temp = result[u'geo_administrative_area_level_1']
    temp = temp.upper() if temp else u''
    city_info[u'region'] = temp

    # Get the predefined city list for a certain country
    if not big_cities:
        big_cities = {}
    if city_info['code2'] in big_cities:
        city_set = big_cities[city_info['code2']]
    else:
        city_set = set({})

    term_list = []
    city_key = []
    # Determine the city: if any of the following fields appears in the predefined city list,
    # if will be set as the city name.
    for k in (u'geo_locality', u'geo_administrative_area_level_2', u'geo_administrative_area_level_3',
              u'geo_administrative_area_level_1', u'geo_sublocality'):
        if not result[k]:
            continue
        elif re.search(u'\d+', result[k].strip()):
            # Ignore the terms made of digits
            continue
        elif result[k].upper() in city_set:
            city_info[u'city'] = result[k].upper()
            city_key.append(k)
            break
        else:
            db.query(str.format('SELECT * FROM city WHERE city_e="{0}" AND region_e="{1}" AND country_e="{2}"',
                                result[k].encode('utf-8'), city_info[u'region'].encode('utf-8'),
                                city_info[u'country'].encode('utf-8')))
            record_set = db.store_result()
            if record_set.num_rows() > 0:
                city_info[u'city'] = result[k].upper()
                city_key.append(k)
                break
            else:
                # Remove level-1 records from the Bing search list, because they are prone to false positive.
                if k != u'geo_administrative_area_level_1':
                    term_list.append(result[k].upper())
                    city_key.append(k)

    is_default = False
    # term_list = tuple(set(term_list))
    if u'city' not in city_info:
        if len(term_list) == 0:
            return None
        elif len(term_list) == 1:
            # Only locality exits
            city_info[u'city'] = term_list[0]
        else:
            # Determine the city name via Google Search
            # term_list = tuple(set(term_list))
            cnt_list = results_stats(term_list)
            # time.sleep(0.5)
            d = dict((term_list[i], (cnt_list[i], city_key[i])) for i in xrange(len(cnt_list)))
            temp = tuple((term_list[i], cnt_list[i], city_key[i]) for i in xrange(len(cnt_list)))
            sorted_key = sorted(temp, key=lambda k: d[k[0]][0], reverse=True)
            if sorted_key[1][1] == 0 or \
                                    sorted_key[1][1] / sorted_key[0][1] > ratio_threshold:
                city_info[u'city'] = sorted_key[0][0].upper()
                city_key = (sorted_key[0][2],)
            else:
                if result[u'geo_locality']:
                    city_info[u'city'] = result[u'geo_locality'].upper()
                    city_key = (u'geo_locality',)
                    is_default = True
                elif result[u'geo_sublocality']:
                    city_info[u'city'] = result[u'geo_sublocality'].upper()
                    city_key = (u'geo_locality',)
                    is_default = True

    if u'city' not in city_info:
        return None


    # Check whether the city exists
    db.query(str.format('SELECT idcity FROM city WHERE city_e="{0}" AND region_e="{1}" AND country_e="{2}"',
                        *map(lambda k: city_info[k].encode('utf-8'), (u'city', u'region', u'country'))))
    record_set = db.store_result()
    if record_set.num_rows() == 0:
        # Get the chinese version
        geo_result = geosense.geocode(addr=result[u'geo_formatted_address'], lang='zh')
        city_info[u'city_c'] = u''
        city_info[u'region_c'] = u''
        city_info[u'country_c'] = u''
        city_key = city_key[0][4:]
        if geo_result and len(geo_result) > 0:
            geo_result = geo_result[0][u'address_components']
            for item in geo_result:
                if city_key in item[u'types']:
                    city_info[u'city_c'] = remove_geo_suffix(item[u'long_name'])
                    continue
                elif u'administrative_area_level_1' in item[u'types']:
                    city_info[u'region_c'] = remove_geo_suffix(item[u'long_name'])
                elif u'country' in item[u'types']:
                    city_info[u'country_c'] = item[u'long_name']
                    # INSERT
        db.query(str.format('INSERT INTO city (city_e, region_e, country_e, city_c, region_c, country_c) '
                            'VALUES ("{0}","{1}","{2}","{3}","{4}","{5}")',
                            *map(lambda k: city_info[k].encode('utf-8'), (u'city', u'region', u'country', u'city_c',
                                                                          u'region_c', u'country_c'))))
        # Get idcity
        db.query('SELECT MAX(idcity) FROM city')
        record_set = db.store_result()
        if record_set.num_rows() == 0:
            return None
        else:
            return int(record_set.fetch_row()[0][0]), is_default
    else:
        return int(record_set.fetch_row()[0][0]), is_default


def process_geocode_data(db, id_range=None, refine=False, extra_condition=None, block_size=500, db_local=None):
    """
    Deduct city information from the geocode data
    """
    if not db_local:
        db_local = db
    db_local.query('SELECT DISTINCT Code2 FROM world.country')
    code2_list = tuple(v[0].decode('utf-8') for v in db_local.store_result().fetch_row(maxrows=0))
    big_cities = {}
    for code2 in code2_list:
        db_local.query(str.format('SELECT Code FROM world.country WHERE Code2="{0}"', code2.encode('utf-8')))
        record_set = db_local.store_result()
        if record_set.num_rows() > 0:
            record = record_set.fetch_row()
            db_local.query(str.format('SELECT Name FROM world.city WHERE CountryCode="{0}"', record[0][0]))
            record_set = db_local.store_result()
            record = record_set.fetch_row(maxrows=0)
            city_set = set(val[0].decode('utf-8').strip().upper() for val in record)
            # Process items such as NANKING [NANJING]
            alts_a = set({})
            alts_d = set({})
            for city in city_set:
                temp = re.findall(r'(?<=[\[\(])[^\[\]\(\)]+(?=[\)\]])', city)
                if len(temp) > 0:
                    alts_d.add(city)
                    for v in temp:
                        alts_a.add(v.strip())
                    temp = re.sub(r'[\(\[][^\[\]\(\)]+[\)\]]', '', city).strip()
                    alts_a.add(temp)
            for v in alts_d:
                city_set.remove(v)
            for v in alts_a:
                city_set.add(v)
        else:
            city_set = set({})
        big_cities[code2] = city_set

    if not extra_condition:
        extra_condition = []
    if refine:
        cond_st = 'WHERE flag=1 AND geo_country IS NOT NULL AND idcity=0 AND ' \
                  'geo_queried="PASS"'
    else:
        cond_st = 'WHERE flag=1 AND geo_locality IS NOT NULL AND geo_country IS NOT NULL AND idcity=0 AND ' \
                  'geo_queried="PASS"'
    cond = list(extra_condition)
    if id_range:
        cond.append(str.format('idstores>={0} AND idstores<{1}', *id_range))
    if len(cond) > 0:
        cond_st = ' AND '.join((cond_st, ' AND '.join(cond)))

    db.query('SELECT COUNT(idstores) FROM stores ' + cond_st)
    total_cnt = int(db.store_result().fetch_row()[0][0])
    idx = 0
    max_idstores = 0
    logger.info(unicode.format(u'{0} records to be processed', total_cnt))
    while True:
        temp_statement = 'SELECT * FROM stores ' + cond_st + str.format(' AND idstores>={0}',
                                                                        max_idstores + 1) + str.format(' LIMIT {0}',
                                                                                                       block_size)
        db.query(temp_statement)
        record_set = db.store_result()
        if record_set.num_rows() == 0:
            break
        while True:
            result = record_set.fetch_row(how=1)
            if len(result) == 0:
                break
            else:
                result = result[0]
            idx += 1

            result = dict((k.decode('utf-8'), result[k].decode('utf-8') if result[k] else None) for k in result.keys())
            result[u'idstores'] = int(result[u'idstores'])
            max_idstores = result[u'idstores']
            # Calculate the geo-fingerprint, and search in the database
            temp = '|'.join(result[k] if result[k] else u'' for
                            k in ('geo_country', 'geo_administrative_area_level_1',
                                  'geo_administrative_area_level_2',
                                  'geo_administrative_area_level_3', 'geo_locality', 'geo_sublocality')).encode('utf-8')
            m = hashlib.md5()
            m.update(temp)
            fingerprint = m.hexdigest()
            db.query(
                str.format('SELECT idcity, is_default FROM city_fingerprints WHERE fingerprint="{0}"', fingerprint))
            fp_record_set = db.store_result()
            if fp_record_set.num_rows() == 0:
                # Add a new city fingerprint
                temp = insert_new_city(db, result, big_cities=big_cities)
                if not temp:
                    logger.error(unicode.format(u'Failed to fetch city info for idstores={0}', result[u'idstores']))
                    continue
                idcity, is_default = temp
                db.query(str.format('INSERT INTO city_fingerprints (fingerprint, idcity, is_default) VALUES '
                                    '("{0}", {1}, {2})', fingerprint, idcity, is_default))
                # logger.info(unicode.format(u'City fingerprint added: {0}:{1}', fingerprint, idcity))
            else:
                # The city already exists, update the store record
                temp = fp_record_set.fetch_row(how=1)
                idcity = int(temp[0]['idcity'])
                temp = int(temp[0]['is_default'])
                is_default = False if temp == 0 else True

            idstores = int(result[u'idstores'])
            db.query(str.format('UPDATE stores SET idcity={0}, geo_locality_default={1} WHERE idstores={2}',
                                idcity, is_default, idstores))
            logger.info(unicode.format(u'{0}/{1} ({2:.2%}): idstores={3}, idcity={4}, is_default={5}',
                                       idx, total_cnt, float(idx) / total_cnt, idstores, idcity, is_default))
            max_idstores = idstores

    logger.info(u'Done')


def set_stores_city(db, id_range=None, extra_condition=None):
    if not extra_condition:
        extra_condition = []
    cond = list(extra_condition)
    if id_range:
        cond.append(str.format('(idcity>={0} AND idcity<{1})', *id_range))
    statement = 'SELECT idcity FROM city WHERE idcity>0'
    if len(cond) > 0:
        statement = ' AND '.join([statement, ] + cond)
    db.query(statement)
    record_set = db.store_result()
    total_cnt = record_set.num_rows()
    for i in xrange(total_cnt):
        record = record_set.fetch_row(how=1)
        idcity = int(record[0]['idcity'])
        # idstores, idcity = map(int, record[0])
        db.query(str.format('SELECT * FROM city WHERE idcity={0}', idcity))
        temp = db.store_result().fetch_row(maxrows=0, how=1)
        city, region, country, city_c, region_c, country_c = \
            map(lambda k: temp[0][k], ('city_e', 'region_e', 'country_e', 'city_c', 'region_c', 'country_c'))

        db.query(str.format('UPDATE stores SET city_e="{0}", province_e="{1}", country_e="{2}", city_c="{3}", '
                            'province_c="{4}", country_c="{5}" WHERE idcity={6}', city, region, country, city_c,
                            region_c,
                            country_c, idcity))
        logger.info(unicode.format(u'{0}/{1} ({2:.2%}) completed. idcity={3}: {4}, {5}, {6}', i, total_cnt,
                                   float(i) / total_cnt, idcity,
                                   *map(lambda v: v.decode('utf-8'), (city, region, country))))

    logger.info('Done.')


def func_2(db, id_range=None):
    statement = 'SELECT idcity, city_e FROM city WHERE city_c IS NULL'
    if id_range:
        statement += str.format(' AND idcity>={0} AND idcity<{1}', *id_range)
    db.query(statement)
    record_set = db.store_result()
    total_cnt = record_set.num_rows()
    logger.info(unicode.format(u'{0} cities found.', total_cnt))

    for j in xrange(total_cnt):
        temp = record_set.fetch_row()
        idcity = int(temp[0][0])
        city_e = temp[0][1].decode('utf-8')

        db.query(str.format('SELECT * FROM stores WHERE idcity={0} LIMIT 1', idcity))
        data = db.store_result().fetch_row(how=1)
        query_param = data[0]['geo_query_param']

        geo_result_en = geosense.geocode(addr=query_param, lang='en')[0]
        geo_result_zh = geosense.geocode(addr=query_param, lang='zh')
        geo_result_zh = geo_result_zh[0] if geo_result_zh else None
        if not geo_result_zh:
            continue
        geo_info = {}

        # Deduct city_c from city_e
        city_c = None
        key = None
        for k in ('locality', 'administrative_area_level_2', 'administrative_area_level_3'):
            for i in xrange(len(geo_result_en['address_components'])):
                temp = geo_result_en['address_components'][i]
                if k in temp['types']:
                    if city_e == temp['long_name'].upper():
                        key = k
                        break
            if key:
                break

        if key:
            for i in xrange(len(geo_result_zh['address_components'])):
                temp = geo_result_zh['address_components'][i]
                if key in temp['types']:
                    # city_c = re.sub(u'省$', u'', temp['long_name'].upper())
                    city_c = temp['long_name'].upper()
                    break

        geo_info[u'city_e'] = city_e
        if city_c:
            geo_info[u'city_c'] = city_c

        for item in geo_result_zh['address_components']:
            kl, ks = None, None
            if 'country' in item['types']:
                kl = 'country_c'
                ks = kl + '_short'
            elif 'administrative_area_level_1' in item['types']:
                kl = 'region_c'
                ks = kl + '_short'

            if kl and ks:
                geo_info[kl], geo_info[ks] = (item[k].upper() for k in ('long_name', 'short_name'))

        # Update the database
        temp = u', '.join(map(lambda k: unicode.format(u'{0}="{1}"', k, geo_info[k]), geo_info.keys()))
        statement = (u'UPDATE city SET ' + temp + unicode.format(u' WHERE idcity={0}', idcity)).encode('utf-8')
        db.query(statement)

        logger.info(
            unicode.format(u'{0}/{1} ({2:.2%}) completed: idcity={3}', j, total_cnt, float(j) / total_cnt, idcity))

    logger.info(u'Done.')


def remove_geo_suffix(term):
    new_term = term
    # if re.search(u'行政区', term[-3:]):
    #     new_term = term[:-3]
    if re.search(u'自治', term):
        pass
    elif (term[-1] == u'市' or term[-1] == u'州' or term[-1] == u'区' or term[-1] == u'省') and len(term) > 2:
        new_term = term[:-1]
    return new_term


def city_display_name(db):
    """
    Determine column 'display_name' from geocode data
    :param db:
    """

    pass


def import_manual_records(db, filename):
    def splitter(text, delimeter=u','):
        terms = []
        idx = 0
        in_quotes = False
        for i in xrange(len(text)):
            if text[i] == u'"':
                in_quotes = not in_quotes
            elif text[i] == u',' and not in_quotes:
                temp = text[idx:i].strip()
                if len(temp) >= 2 and temp[0] == u'"' and temp[-1] == u'"':
                    temp = temp[1:-1].strip()
                terms.append(temp)
                idx = i + 1
        temp = text[idx:].strip()
        if len(temp) >= 2 and temp[0] == u'"' and temp[-1] == u'"':
            temp = temp[1:-1].strip()
        terms.append(temp)
        return terms


    with open(filename, mode='r') as f:
        data = list(f)
        header = splitter(data[0].decode('utf-8'))[:-1]
        header += (u'lat', u'lng')
        for line in data[1:]:
            line_data = splitter(line.decode('utf-8'))
            loc = line_data[-1]
            del (line_data[-1])
            line_data += map(float, loc.split(','))
            info_map = dict((header[i], line_data[i]) for i in xrange(len(header)))
            key_list = info_map.keys()
            value_list = []
            for k in key_list:
                if isinstance(info_map[k], int) or isinstance(info_map[k], float):
                    value_list.append(unicode(info_map[k]))
                else:
                    value_list.append(unicode.format(u'"{0}"', info_map[k]))
            statement = (
                u'INSERT INTO stores (' + u','.join(key_list) + u') VALUES (' + u','.join(value_list) + u')').encode(
                'utf-8')
            db.query(statement)

    logger.info(u'Done.')


def update_city_info(db, id_range=None, extra_condition=None):
    if not extra_condition:
        extra_condition = []
    cond = list(extra_condition)
    if id_range:
        cond.append(str.format('(idcity>={0} AND idcity<{1})', *id_range))

    statement = 'SELECT * FROM city WHERE (lat IS NULL OR lng IS NULL)'
    if len(cond) > 0:
        statement = ' AND '.join([statement, ] + cond)
    db.query(statement)
    record_set = db.store_result()
    total_cnt = record_set.num_rows()
    logger.info(unicode.format(u'{0} cities found', total_cnt))
    for i in xrange(total_cnt):
        record = record_set.fetch_row(how=1)[0]
        addr = (','.join(map(lambda k: record[k], ('city_e', 'region_e', 'country_e')))).decode('utf-8')
        idcity = int(record['idcity'])
        geo_result = geosense.geocode(addr)
        if not geo_result:
            continue
        geo_result = geo_result[0]['geometry']
        loc_map = {}
        if 'location' in geo_result:
            loc_map[u'lat'] = geo_result['location']['lat']
            loc_map[u'lng'] = geo_result['location']['lng']
        if 'bounds' in geo_result:
            v = geo_result['bounds']
            loc_map[u'bounds_ne_lat'] = v['northeast']['lat']
            loc_map[u'bounds_ne_lng'] = v['northeast']['lng']
            loc_map[u'bounds_sw_lat'] = v['southwest']['lat']
            loc_map[u'bounds_sw_lng'] = v['southwest']['lng']
        if 'viewport' in geo_result:
            v = geo_result['viewport']
            loc_map[u'viewport_ne_lat'] = v['northeast']['lat']
            loc_map[u'viewport_ne_lng'] = v['northeast']['lng']
            loc_map[u'viewport_sw_lat'] = v['southwest']['lat']
            loc_map[u'viewport_sw_lng'] = v['southwest']['lng']
        for k in loc_map:
            loc_map[k] = float(loc_map[k])

        statement = ', '.join(str.format('{0}={1}', k, loc_map[k]) for k in loc_map.keys())
        statement = 'UPDATE city SET ' + statement + str.format(' WHERE idcity={0}', idcity)
        db.query(statement)
        logger.info(
            unicode.format(u'{0}/{1} ({2:.2%}) completed: idcity={3}', i, total_cnt, float(i) / total_cnt, idcity))

    logger.info(u'Done')


def tel_formatter(db, id_range=None, extra_condition=None):
    statement = 'SELECT idstores, tel FROM stores'
    # statement_c = 'SELECT COUNT(idstores) FROM stores'
    cond_list = []
    if id_range:
        cond_list.append(str.format('idstores>={0} AND idstores<{1}', *id_range))
    if extra_condition:
        cond_list.append(extra_condition)
    if len(cond_list) > 0:
        statement += str.format(' WHERE {0}', ' AND '.join(cond_list))
        # statement_c += str.format(' WHERE {0}', ' AND '.join(cond_list))

    # db.query(statement_c)
    # total_cnt = int(db.store_result().fetch_row()[0][0])
    db.query(statement)
    record_set = db.store_result()
    total_cnt = record_set.num_rows()
    logger.info(unicode.format(u'{0} records found', total_cnt))
    for i in xrange(total_cnt):
        record = record_set.fetch_row(how=1)[0]
        idstores = int(record['idstores'])
        tel = record['tel']
        if not tel:
            continue

        if not re.search(r'\d{2,}', tel):
            new_tel = ''
        else:
            new_tel = tel.strip()
            if re.search(r'^(ph|phone|tel)([\.:\s])*', new_tel, re.I):
                new_tel = re.sub(r'^(ph|phone|tel)([\.:\s])*', r'', new_tel, flags=re.I).strip()
            new_tel = re.sub(r'\s{2,}', ' ', new_tel)

        if new_tel == tel:
            continue
        db.query(str.format('UPDATE stores SET tel="{0}" WHERE idstores={1}', new_tel, idstores))
        logger.info(unicode.format(u'{0}/{1} ({2:.2%}) completed: idstores={3}, tel={4}', i, total_cnt,
                                   float(i) / total_cnt, idstores, new_tel.decode('utf-8')))

    logger.info(u'Done')


def gen_display_name(db, id_range=None, extra_condition=None):
    pass


def func(db):
    db.query('SELECT idcity FROM city WHERE country_e="CHINA"')
    record_set = db.store_result()
    idcity_list = tuple(int(v[0]) for v in record_set.fetch_row(maxrows=0))

    db.query(str.format('DELETE FROM city WHERE {0}', ' OR '.join(str.format('idcity={0}', v) for v in idcity_list)))
    db.query(str.format('DELETE FROM city_fingerprints WHERE {0}',
                        ' OR '.join(str.format('idcity={0}', v) for v in idcity_list)))


if __name__ == "__main__":
    host, database, user, passwd, port = '127.0.0.1', 'brand_stores', 'root', '', 3306
    for i in xrange(1, len(sys.argv), 2):
        token = sys.argv[i]
        if token[0] != '-':
            logger.error(unicode.format(u'Invalid parameter: {0}', token))
            exit(-1)
        cmd = token[1:]
        value = sys.argv[i + 1]
        if cmd == 'u':
            user = value
        elif cmd == 'P':
            port = int(value)
        elif cmd == 'p':
            passwd = value
        elif cmd == 'd':
            database = value
        elif cmd == 'h':
            host = value

    # geocode_query()
    # update_geo_shift(
    #     excluded_brand={10004, 10095, 10123, 10170, 10196, 10227, 10277, 10279, 10318, 10359, 10361, 10363})

    # update_city_info()

    # db_local = _mysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='brand_stores')
    # db_local.query("SET NAMES 'utf8'")

    db = _mysql.connect(host=host, port=port, user=user, passwd=passwd, db=database)
    db.query("SET NAMES 'utf8'")

    # tel_formatter(db)
    geocode_query(db, extra_condition=('geo_country="CHINA"',))
    process_geocode_data(db, refine=False, extra_condition=('geo_country="CHINA"',), db_local=db)
    # set_stores_city(db, extra_condition=('(country_e="CHINA")',))
    # update_geo_shift(db, overwrite=True)
    # update_city_info(db, extra_condition=('(country_e="CHINA")',))
    # import_manual_records(db, u'10237-Maison Martin Margiela.csv')
    # func(db)

    db.close()
    # db_local.close()

    # results_stats(('Beijing,China',))