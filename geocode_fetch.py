# coding=utf-8
import _mysql
import logging
import logging.config
from math import cos, sin, acos
import hashlib
import re
import time
import sys
import datetime
import common as cm
from pyquery import PyQuery as pq
import geosense
import codecs

__author__ = 'Zephyre'
# Fetch the geocode information for every store record, based on its address and location.

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


def load_city_aggr(db, filename):
    # db.query('SET AUTOCOMMIT=0')
    rules = {}
    with open(filename, 'r') as f:
        with open(u'result.txt', 'w') as fo:
            idx = 0
            for line in f.readlines():
                idx += 1
                data = tuple(int(val) for val in re.findall(ur'(\d+):', line))
                if len(data) <= 2:
                    print(unicode.format(u'Something wrong in line {0}', idx))
                    continue

                target_id = data[data[0]]
                id_changed = list(filter(lambda val: val != target_id, data[1:]))
                if target_id in rules:
                    rules[target_id] += id_changed
                else:
                    rules[target_id] = id_changed
                text = unicode.format(u'{0} <= {1}', target_id, u'|'.join(unicode(v) for v in id_changed))
                print(text)
                fo.write(unicode.format(u'{0}\n', text))

    # for target_id in rules:
    #     print(
    #         unicode.format(u'Aggregates: {0} <= [{1}]', target_id,
    #                        u'|'.join(unicode(temp) for temp in rules[target_id])))
    #     for idcity in rules[target_id]:
    #         db.query(str.format('UPDATE stores SET idcity={0} WHERE idcity={1}', target_id, idcity))
    #     db.query(str.format('DELETE FROM city WHERE {0}',
    #                         ' || '.join(str.format('idcity={0}', val) for val in rules[target_id])))
    #     db.commit()

    print(u'Done')


def detect_city_aggr(db, threshold=0, threshold2=0.1, filename=u'city_aggr.txt'):
    def get_pairs(vals):
        vals = tuple(vals)
        cnt = len(vals)
        pairs = []
        for i1 in xrange(cnt):
            for i2 in xrange(i1 + 1, cnt):
                pairs.append({vals[i1], vals[i2]})
        return pairs

    db.query('SELECT DISTINCT country_e FROM city')
    country_list = tuple(temp[0].decode('utf-8') for temp in db.store_result().fetch_row(maxrows=0))
    with codecs.open(filename, 'wb', 'utf-8') as output:
        for c in country_list:
            db.query(unicode.format(
                u'SELECT idcity, lat, lng FROM city WHERE country_e="{0}" && lat IS NOT NULL && lng IS NOT NULL',
                c).encode('utf-8'))
            results = db.store_result().fetch_row(maxrows=0, how=1)

            city_list = tuple((int(temp['idcity']), float(temp['lat']), float(temp['lng'])) for temp in results)
            city_pairs = get_pairs(city_list)
            candidates = []
            for pair in city_pairs:
                c1, c2 = pair
                p1 = [c1[1], c1[2]]
                p2 = [c2[1], c2[2]]
                dist = calc_distance(p1, p2)
                if dist < threshold2:
                    p2[0], p2[1] = p1[0], p1[1]
                    dist = 0
                if dist <= threshold:
                    candidates.append({u'dist': dist, u'pair': pair})

            s_candidates = sorted(candidates, key=lambda temp: temp[u'dist'])

            identicals = {}
            for val in s_candidates:
                temp = tuple(val[u'pair'])[0]
                loc = (temp[1], temp[2])
                if loc not in identicals:
                    identicals[loc] = set({})
                identicals[loc].add(temp[0])
                identicals[loc].add(tuple(val[u'pair'])[1][0])

            if len(identicals) > 0:
                for loc in identicals:
                    idcity_list = identicals[loc]
                    city_list = []
                    for idcity in idcity_list:
                        db.query(str.format(
                            'SELECT city_e,region_e,country_e,city_c,region_c,country_c FROM city WHERE idcity={0}',
                            idcity))
                        record = db.store_result().fetch_row(how=1)[0]
                        city_list.append(unicode(idcity) + u':' + u'|'.join(
                            (record[key].decode('utf-8') if record[key] else u'') for key in
                            ('city_e', 'region_e', 'country_e', 'city_c', 'region_c', 'country_c')))

                    msg = unicode.format(u',\t\t'.join(city_list))
                    output.write(u':\t\t' + msg + u'\n')
                    print(msg)


def update_geo_shift(db, id_range=None, overwrite=False, extra_condition=None, block_size=500):
    cond = 'WHERE (lat IS NOT NULL AND lng IS NOT NULL AND geo_location_lat IS NOT ' \
           'NULL AND geo_location_lng IS NOT NULL AND flag=1)'
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


def unicodize(record):
    new_record = {}
    for k in record:
        v = record[k]
        k1 = k.decode('utf-8') if isinstance(k, str) else k
        v1 = v.decode('utf-8') if isinstance(v, str) else v
        new_record[k1] = v1
    return new_record


def geocode_query(db, id_range=None, extra_condition=None, overwrite=False, logger=logging.getLogger()):
    """
    Perform the geocode_query with the address data.
    :param db:
    :param id_range:
    :raise:
    """

    def get_idstores_list(db, overwrite=False, id_range=None, extra_condition=None):
        """
        Get the list of all idstores.
        :param db:
        :param excluded: ids of excluded brands, iterable.
        :param skip_existing: skip records which already have geocode data?
        :param id_range:
        """
        extra_condition = list(extra_condition) if extra_condition else []
        extra_condition.append('flag=1')
        if not overwrite:
            extra_condition.append('geo_queried="N/A"')
        if id_range:
            extra_condition.append(str.format('(idstores BETWEEN {0} AND {1})', *id_range))

        statement = 'SELECT idstores FROM stores'
        if len(extra_condition) > 0:
            statement = str.format('{0} WHERE {1}', statement, ' && '.join(extra_condition))
        statement += ' ORDER BY idstores'
        db.query(statement)
        result = db.store_result().fetch_row(maxrows=0)
        store_list = tuple(int(v[0]) for v in result)
        logger.info(str.format('{0} records fetched: {1}...', len(store_list),
                               ', '.join((str(brand_id) for brand_id in store_list[:10]))))
        return store_list

    def gen_search_list(result):
        """
        Generate search terms in the following formats: addr,region,country
        :param result:
        """
        country, province, city = (result[key] for key in ('country_e', 'province_e', 'city_e'))
        if result['addr_e']:
            addr = result['addr_e']
        elif result['addr_c']:
            addr = result['addr_c']
        elif result['addr_l']:
            addr = result['addr_l']
        else:
            logger.error(unicode.format(u'Address missing for idstores={0}', idstores))
            db.query(str.format('UPDATE stores SET geo_queried="FAIL", update_time="{1}", modified=1 WHERE '
                                'idstores={0}', idstores, datetime.datetime.now()))
            return ()
            # Remove postal codes from the address
        addr = re.sub(ur'\d{4,}', u'', addr)
        city = city if city else u''
        country = country if country else u''
        return ','.join((addr, city, country)), addr


    def filter_geo_results(results, loc_o):
        """
        Find the nearest result to loc_o
        :param results:
        """
        distances = tuple(map(lambda r: calc_distance(loc_o, tuple(float(r['geometry']['location'][k])
                                                                   for k in ('lat', 'lng'))), results))

        return dict((idx, distances[idx]) for idx in xrange(len(results)))


    extra_condition = list(extra_condition) if extra_condition else []
    db.query('SET AUTOCOMMIT=1')
    idstores_list = get_idstores_list(db, id_range=id_range, extra_condition=extra_condition, overwrite=overwrite)
    total_cnt = len(idstores_list)
    for i in xrange(total_cnt):
        idstores = idstores_list[i]
        db.query(str.format('SELECT * FROM stores WHERE idstores={0}', idstores))
        result = unicodize(db.store_result().fetch_row(how=1)[0])

        if result['lat'] and result['lng']:
            loc_o = tuple(float(result[key]) for key in ('lat', 'lng'))
        else:
            loc_o = None

        geo_result_list = []
        search_term_list = []
        for term in gen_search_list(result):
            temp = geosense.geocode2(addr=term, retry=5, logger=logger)
            geo_result_list += temp
            search_term_list += [term] * len(temp)
            # If loc_o doesn't exist, there's no need to get all the geo_results
            if len(geo_result_list) >= 1:# and not loc_o:
                break

        if len(geo_result_list) == 0:
            logger.error(unicode.format(u'No geocode result returned for {0}', idstores))
            db.query(str.format('UPDATE stores SET geo_queried="FAIL", update_time="{1}", modified=1 '
                                'WHERE idstores={0}', idstores, datetime.datetime.now()))
            continue
        # elif loc_o:
        #     distances = filter_geo_results(geo_result_list, loc_o)
        #     sorted_distances = sorted(distances, key=lambda key: distances[key])
        #     dist_threshold = 100
        #     if sorted_distances[0] < dist_threshold:
        #         geo_result = geo_result_list[sorted_distances[0]]
        #         search_term = search_term_list[sorted_distances[0]]
        #     else:
        #         geo_result = geo_result_list[0]
        #         search_term = search_term_list[0]
        else:
            geo_result = geo_result_list[0]
            search_term = search_term_list[0]

        geo_keys = {u'locality', u'sublocality', u'street_number', u'country', u'postal_code', u'establishment',
                    u'neighborhood', u'postal_town'}
        for j in xrange(1, 4):
            geo_keys.add(unicode.format(u'administrative_area_level_{0}', j))
        geo_dict = dict((unicode.format(u'geo_{0}', v), None) for v in geo_keys)
        for k in geo_keys:
            geo_dict[unicode.format(u'geo_{0}_short', k)] = None
        geo_keys = {u'formatted_address', u'query_param'}
        for k in geo_keys:
            geo_dict[unicode.format(u'geo_{0}', k)] = None
        geo_dict[u'addr_hash'] = None
        geo_keys = ((u'bounds', u'viewport'), (u'ne', u'sw'), (u'lat', u'lng'))
        for k in (unicode.format(u'geo_{0}_{1}_{2}', k0, k1, k2) for k0 in geo_keys[0] for k1 in geo_keys[1]
                  for k2 in geo_keys[2]):
            geo_dict[k] = None
        geo_dict[u'geo_shift'] = None
        geo_dict[u'geo_location_lat'], geo_dict[u'geo_location_lng'] = None, None


        # Make sure all the keys in geo_result are registered
        if not (set(geo_result.keys()) - {'administrative_info'} <= {'address_components', 'formatted_address',
                                                                     'geometry', 'partial_match', 'types',
                                                                     'postcode_localities'}):
            raise LookupError(unicode.format(u'Unknown fields {0} for {1} / address={2}', set(geo_result.keys()),
                                             idstores, search_term))

        # Parse the geocode result which is in JSON format.
        if 'address_components' not in geo_result:
            logger.error(unicode.format(u'No address_components found for {0} / address={1}', idstores, search_term))
            db.query(str.format('UPDATE stores SET geo_queried="FAIL", update_time="{1}", modified=1 '
                                'WHERE idstores={0}', idstores, datetime.datetime.now()))
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
                                                 idstores, search_term))
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
            logger.error(unicode.format(u'No geometry found for {0} / address={1}', idstores, search_term))
            db.query(str.format('UPDATE stores SET geo_queried="FAIL", update_time="{1}", modified=1 WHERE '
                                'idstores={0}', idstores, datetime.datetime.now()))
            continue
        geometry = geo_result['geometry']
        if 'location' not in geometry:
            logger.error(unicode.format(u'No location data found for {0} / address={1}', idstores, search_term))
            db.query(str.format(u'UPDATE stores SET geo_queried="FAIL" WHERE idstores={0}', idstores))
            continue

        # Make sure all keys in geometry are registered
        if not (set(geometry.keys()) <= {'bounds', 'viewport', 'location_type', 'location'}):
            raise LookupError(unicode.format(u'Unknown fields {0} for {1} / address={2}', geometry.keys(),
                                             idstores, search_term))
        geo_dict['geo_location_lat'] = float(geometry['location']['lat'])
        geo_dict['geo_location_lng'] = float(geometry['location']['lng'])
        if 'bounds' in geometry:
            geo_bounds = geo_result['geometry']['bounds']
            geo_dict['geo_bounds_ne_lat'] = float(geo_bounds['northeast']['lat'])
            geo_dict['geo_bounds_ne_lng'] = float(geo_bounds['northeast']['lng'])
            geo_dict['geo_bounds_sw_lat'] = float(geo_bounds['southwest']['lat'])
            geo_dict['geo_bounds_sw_lng'] = float(geo_bounds['southwest']['lng'])
        if 'viewport' in geometry:
            geo_viewport = geometry['viewport']
            geo_dict['geo_viewport_ne_lat'] = float(geo_viewport['northeast']['lat'])
            geo_dict['geo_viewport_ne_lng'] = float(geo_viewport['northeast']['lng'])
            geo_dict['geo_viewport_sw_lat'] = float(geo_viewport['southwest']['lat'])
            geo_dict['geo_viewport_sw_lng'] = float(geo_viewport['southwest']['lng'])
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
            if not v:
                term_list.append(unicode.format(u'{0}=NULL', k))
            elif isinstance(v, float) or isinstance(v, int):
                term_list.append(unicode.format(u'{0}={1}', k, v))
            else:
                v = re.sub(ur'(?<!\\)"', ur'\\"', unicode(v))
                term_list.append(unicode.format(u'{0}="{1}"', k, v))
        term_list.append(unicode.format(u'update_time="{0}"', datetime.datetime.now()))

        update_str = unicode.format(u'UPDATE spider_stores.stores SET ') + u', '.join(term_list) + \
                     unicode.format(u' WHERE idstores={0}', idstores)
        db.query(update_str.encode('utf-8'))
        logger.info(unicode.format(u'{0}/{1} completed({2:.2%}): idstores={3}', i, total_cnt,
                                   float(i) / total_cnt, idstores))

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
    admin_candidates = [u'geo_locality', u'geo_administrative_area_level_2', u'geo_administrative_area_level_3',
                        u'geo_sublocality']
    if result[u'geo_administrative_area_level_1'] and \
                    result[u'geo_administrative_area_level_1'].upper().strip() in \
                    {u'TOKYO', u'SHANGHAI', u'BEIJING', u'CHONGQING', u'TIANJIN'}:
        admin_candidates.insert(0, u'geo_administrative_area_level_1')
        temp = result[u'geo_administrative_area_level_1'].upper().strip()
        city_set.add(temp)
        city_info[u'region'] = temp
    if result[u'geo_country'] and result[u'geo_country'].upper().strip() in {u'HONG KONG', u'MACAU'}:
        admin_candidates.insert(0, u'geo_country')
        temp = result[u'geo_country'].upper().strip()
        city_set.add(temp)
        city_info[u'region'] = temp
        city_info[u'country'] = u'CHINA'
        city_info[u'Code2'] = u'CN'
    elif result[u'geo_country'] and result[u'geo_country'].upper().strip() == u'TAIWAN':
        city_info[u'region'] = result[u'geo_country'].upper().strip()
        city_info[u'country'] = u'CHINA'
        city_info[u'Code2'] = u'CN'
        # postal_town具有最高优先级，确定城市名称
    if not result[u'geo_postal_town']:
        for k in admin_candidates:
            if not result[k]:
                continue
            elif re.search(ur'\d+', result[k].strip()):
                # Ignore the terms made of digits
                continue
            elif result[k].upper() in city_set:
                city_info[u'city'] = result[k].upper()
                city_key.append(k)
                break
            elif re.search(ur'city\s*$', result[k], flags=re.I):
                city_info[u'city'] = result[k].upper()
                city_set.add(city_info[u'city'])
                city_key.append(k)
                break
            else:
                db.query(unicode.format(u'SELECT * FROM city WHERE city_e="{0}" AND region_e="{1}" AND country_e="{2}"',
                                        result[k], city_info[u'region'], city_info[u'country']).encode('utf-8'))
                record_set = db.store_result()
                if record_set.num_rows() > 0:
                    city_info[u'city'] = result[k].upper()
                    city_set.add(city_info[u'city'])
                    city_key.append(k)
                    break
                else:
                    term_list.append(result[k].upper())
                    city_key.append(k)
    else:
        postal_town = re.sub(ur'\d+', u' ', result[u'geo_postal_town'])
        postal_town = re.sub(ur'\s+', u' ', postal_town).strip().upper()
        city_info[u'city'] = postal_town
        city_set.add(postal_town)
        city_key.append(u'geo_postal_town')

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
        # # Get the chinese version
        # geo_result = geosense.geocode(addr=result[u'geo_formatted_address'], lang='zh')
        # city_info[u'city_c'] = u''
        # city_info[u'region_c'] = u''
        # city_info[u'country_c'] = u''
        # city_key = city_key[0][4:]
        # if geo_result and len(geo_result) > 0:
        #     geo_result = geo_result[0][u'address_components']
        #     for item in geo_result:
        #         if city_key in item[u'types']:
        #             city_info[u'city_c'] = remove_geo_suffix(item[u'long_name'])
        #             continue
        #         elif u'administrative_area_level_1' in item[u'types']:
        #             city_info[u'region_c'] = remove_geo_suffix(item[u'long_name'])
        #         elif u'country' in item[u'types']:
        #             city_info[u'country_c'] = item[u'long_name']
        #             # INSERT
        # db.query(str.format('INSERT INTO city (city_e, region_e, country_e, city_c, region_c, country_c) '
        #                     'VALUES ("{0}","{1}","{2}","{3}","{4}","{5}")',
        #                     *map(lambda k: city_info[k].encode('utf-8'), (u'city', u'region', u'country', u'city_c',
        #                                                                   u'region_c', u'country_c'))))

        db.query(str.format('INSERT INTO city (city_e, region_e, country_e) '
                            'VALUES ("{0}","{1}","{2}")',
                            *map(lambda k: city_info[k].encode('utf-8'), (u'city', u'region', u'country'))))
        # Get idcity
        db.query(str.format('SELECT idcity FROM city WHERE city_e="{0}" && region_e="{1}" && country_e="{2}"',
                            *map(lambda k: city_info[k].encode('utf-8'), (u'city', u'region', u'country'))))
        record_set = db.store_result()
        if record_set.num_rows() == 0:
            return None
        else:
            return int(record_set.fetch_row()[0][0]), is_default
    else:
        return int(record_set.fetch_row()[0][0]), is_default


def get_addr_hash(db):
    db.query('SELECT DISTINCT addr_hash, idcity FROM stores')
    record_set = db.store_result()
    results = record_set.fetch_row(maxrows=0, how=1)
    addr_hash = {}
    for item in results:
        if not item['addr_hash']:
            continue
        addr_hash[item['addr_hash']] = int(item['idcity'])
    return addr_hash


def process_geocode_data(db, id_range=None, refine=False, extra_condition=None, block_size=500, db_local=None):
    """
    Deduct city information from the geocode data
    """
    db.query('SET AUTOCOMMIT=1')
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

    addr_hash_dict = get_addr_hash(db)

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
        db.commit()
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
                result = unicodize(result[0])
            idx += 1

            result = dict((k, result[k] if result[k] else None) for k in result.keys())
            result[u'idstores'] = int(result[u'idstores'])
            idstores = result[u'idstores']
            max_idstores = result[u'idstores']
            # Calculate the geo-fingerprint, and search in the database
            temp = '|'.join(result[k] if result[k] else u'' for
                            k in ('geo_country', 'geo_administrative_area_level_1',
                                  'geo_administrative_area_level_2',
                                  'geo_administrative_area_level_3', 'geo_locality', 'geo_sublocality')).encode('utf-8')
            m = hashlib.md5()
            m.update(temp)
            fingerprint = m.hexdigest()
            if fingerprint in addr_hash_dict:
                # The city already exists, update the store record
                idcity = addr_hash_dict[fingerprint]
                is_default = False
            else:
                # Add a new city fingerprint
                temp = insert_new_city(db, result, big_cities=big_cities)
                if not temp:
                    logger.error(unicode.format(u'Failed to fetch city info for idstores={0}', result[u'idstores']))
                    continue
                idcity, is_default = temp
                addr_hash_dict[fingerprint] = idcity

            db.query(
                str.format('UPDATE stores SET idcity={0}, geo_locality_default={1}, addr_hash="{3}", '
                           'update_time="{4}", modified=1 WHERE idstores={2}',
                           idcity, is_default, idstores, fingerprint, datetime.datetime.now()))
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
    elif term[-1] in (u'省', u'市') and len(term) > 2:
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


def update_city_info(db, id_range=None, extra_condition=None, overwrite=False):
    db.query('SET AUTOCOMMIT=1')
    extra_condition = list(extra_condition) if extra_condition else []
    if not overwrite:
        extra_condition.append('(lat IS NULL || lng IS NULL)')
    if id_range:
        extra_condition.append(str.format('(idcity BETWEEN {0} AND {1})', *id_range))

    statement = str.format('SELECT * FROM city WHERE {0}', ' && '.join(extra_condition))
    db.query(statement)
    record_set = db.store_result()
    total_cnt = record_set.num_rows()
    logger.info(unicode.format(u'{0} cities found', total_cnt))
    for i in xrange(total_cnt):
        record = unicodize(record_set.fetch_row(how=1)[0])
        addr = u','.join(map(lambda k: record[k], ('city_e', 'region_e', 'country_e')))
        idcity = int(record['idcity'])
        geo_result = geosense.geocode2(addr)
        if len(geo_result) == 0:
            continue
        addr_en = geo_result[0]['address_components']
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

        # Chinese version
        addr_zh_map = {}
        geo_result_zh = geosense.geocode2(addr, lang='zh')
        if len(geo_result_zh) > 0:
            addr_zh = geo_result_zh[0]['address_components']
            city_key = None
            region_key = None
            country_key = None
            for addr in addr_en:
                if record[u'city_e'] in (addr[key].upper() for key in ('long_name', 'short_name')):
                    city_key = addr['types'][0]
                    break

            for addr in addr_zh:
                if u'country' in addr[u'types']:
                    addr_zh_map[u'country_c'] = addr[u'long_name']
                    addr_zh_map[u'country_c_short'] = addr[u'short_name']
                elif u'administrative_area_level_1' in addr[u'types']:
                    addr_zh_map[u'region_c'] = remove_geo_suffix(addr[u'long_name'])
                    addr_zh_map[u'region_c_short'] = remove_geo_suffix(addr[u'short_name'])

                if city_key in addr[u'types']:
                    addr_zh_map[u'city_c'] = remove_geo_suffix(addr[u'long_name'])
                    addr_zh_map[u'city_c_short'] = remove_geo_suffix(addr[u'short_name'])

            if u'country_c' in addr_zh_map and addr_zh_map[u'country_c'] in {u'台灣', u'澳門', u'香港', u'澳门', u'台湾'}:
                addr_zh_map[u'country_c'] = u'中国'
                addr_zh_map[u'country_c_short'] = u'CN'

        statement = ', '.join(str.format('{0}={1}', k, loc_map[k]) for k in loc_map.keys())
        statement += (
            u', ' + u', '.join(
                unicode.format(u'{0}="{1}"', k, addr_zh_map[k].upper().strip()) for k in addr_zh_map.keys())).encode(
            'utf-8')
        statement = 'UPDATE city SET ' + statement + str.format(' WHERE idcity={0}', idcity)
        db.query(statement)
        logger.info(
            unicode.format(u'{0}/{1} ({2:.2%}) completed: idcity={3}', i, total_cnt, float(i) / total_cnt, idcity))

    logger.info(u'Done')

# TODO email_formatter

def tel_formatter(db, field='tel', id_range=None, extra_condition=None, logger=None):
    statement = 'SELECT idstores, tel FROM stores'
    # statement_c = 'SELECT COUNT(idstores) FROM stores'
    cond_list = []
    if id_range:
        cond_list.append(str.format('idstores>={0} AND idstores<{1}', *id_range))
    if extra_condition:
        cond_list += extra_condition

    statement = str.format('select idstores, {0} from stores where tel regexp '
                           '"^(ph|phone|tel|fax|:|[[:space:]]|call[[:space:]]+center|call[[:space:]]+centre|电话|電話|传真|：)+"',
                           field)
    if len(cond_list) > 0:
        statement = str.format('{0} AND {1}', statement, ' AND '.join(cond_list))

    db.query(statement)
    updates = []
    updates += db.store_result().fetch_row(maxrows=0)
    db.query(str.format('select idstores, {0} from stores where tel regexp "[（）]"', field))
    updates += db.store_result().fetch_row(maxrows=0)
    cnt = 0
    for record in updates:
        cnt += 1
        idstores, tel = int(record[0]), record[1]
        new_tel = re.sub(r'^(ph|phone|tel|fax|:|\s|call\s+center|call\s+centre|电话|電話|传真|：)+', '', tel,
                         flags=re.I).strip()
        new_tel = re.sub(r'（', '(', new_tel, flags=re.I)
        new_tel = re.sub(r'）', ')', new_tel, flags=re.I)
        db.query(str.format('UPDATE stores SET tel="{0}" WHERE idstores={1}', new_tel, idstores))
        if logger:
            logger.info(unicode.format(u'#{0} {1} changed at idstores={4}: {2} => {3}', cnt, field.decode('utf-8'),
                                       tel.decode('utf-8'), new_tel.decode('utf-8'), idstores))

    db.commit()
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


def proc_extra_cond(extra_cond):
    if not extra_cond:
        extra_cond = []
    for i in xrange(len(extra_cond)):
        term = extra_cond[i].strip()
        term = term if isinstance(term, unicode) else term.decode('utf-8')
        if not re.search(ur'^\(.*\)$', term):
            term = unicode.format(u'({0})', term)
        extra_cond[i] = term
    return extra_cond


def gen_addr_hash(db, block_size=50):
    """
    Generate addr_hash for each record.
    :param db:
    """
    db.query('SELECT fingerprint FROM city_fingerprints')
    record_set = db.store_result()
    fingerprint_set = set(v[0] for v in record_set.fetch_row(maxrows=0))

    db.query('SELECT COUNT(*) FROM stores WHERE geo_queried="PASS" AND addr_hash IS NULL')
    total_cnt = int(db.store_result().fetch_row(maxrows=0)[0][0])
    idx = 0
    max_idstores = 0
    while True:
        if idx == total_cnt:
            break
        db.query(str.format('SELECT * FROM stores WHERE geo_queried="PASS" AND addr_hash IS NULL AND idstores>{0} '
                            'ORDER BY idstores LIMIT {1}', max_idstores, block_size))
        record_set = db.store_result()
        for i in xrange(record_set.num_rows()):
        # while True:
            result = record_set.fetch_row(how=1)[0]
            # if len(result) == 0:
            #     break
            # result = result[0]
            idx += 1
            idstores = int(result['idstores'])
            max_idstores = idstores

            temp = '|'.join(result[k] if result[k] else '' for
                            k in ('geo_country', 'geo_administrative_area_level_1',
                                  'geo_administrative_area_level_2',
                                  'geo_administrative_area_level_3', 'geo_locality', 'geo_sublocality'))
            m = hashlib.md5()
            m.update(temp)
            fingerprint = m.hexdigest()
            if fingerprint not in fingerprint_set:
                logger.info(unicode.format(u'{0}/{1} ({2:.2%}) completed. idstores={3}, hash=None', idx, total_cnt,
                                           float(i) / total_cnt, idstores))
                continue
            statement = str.format('UPDATE stores SET addr_hash="{0}" WHERE idstores={1}', fingerprint, idstores)
            db.query(statement)
            logger.info(unicode.format(u'{0}/{1} ({2:.2%}) completed. idstores={3}, hash={4}', idx, total_cnt,
                                       float(idx) / total_cnt, idstores, fingerprint))

        db.commit()

    logger.info(u'Done')


def clean_up(db, extra_cond=None):
    """
    Clean orphan records in city and city_fingerprints
    :param db:
    """
    db.query('SELECT idcity FROM city')
    record_set = db.store_result()
    idcity_list = tuple(int(v[0]) for v in record_set.fetch_row(maxrows=0))

    db.query('SELECT DISTINCT idcity FROM stores')
    record_set = db.store_result()
    valid_idcity_set = set(int(v[0]) for v in record_set.fetch_row(maxrows=0))

    city_del = []
    for idcity in idcity_list:
        if idcity not in valid_idcity_set:
            city_del.append(idcity)
            logger.info(unicode.format(u'idcity={0} not present in stores', idcity))
    if len(city_del) > 0:
        cond = ' OR '.join(str.format('idcity={0}', v) for v in city_del)
        statement = 'DELETE FROM city WHERE ' + cond
        db.query(statement)
        db.commit()

    logger.info(u'Done')


def proc_tokyo(db):
    db.query('SELECT idcity FROM city WHERE country_e="JAPAN" && region_e LIKE "%TOKYO%"')
    idcity_list = tuple(int(v[0]) for v in db.store_result().fetch_row(maxrows=0))
    db.query(str.format('UPDATE stores SET idcity=9367 WHERE {0}',
                        ' OR '.join((str.format('idcity={0}', v) for v in idcity_list))))
    db.commit()


if __name__ == "__main__":
    logging.config.fileConfig('geocode_fetch.cfg')
    logger = logging.getLogger('firenzeLogger')

    host, database, user, passwd, port = '127.0.0.1', 'spider_stores', 'root', '', 3306
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

    db = _mysql.connect(host=host, port=port, user=user, passwd=passwd, db=database)
    db.query("SET NAMES 'utf8'")

    # detect_city_aggr(db, threshold2=5)
    # load_city_aggr(db, u'city_mapping_round_1.txt')

    # db.query("SET AUTOCOMMIT=0")

    # tel_formatter(db, extra_condition='(brand_id in (10062, 10127))', logger=logger)
    # tel_formatter(db, field='fax', logger=logger)
    # gen_addr_hash(db)
    # clean_up(db)

    # geocode_query(db, extra_condition=('brand_id in (10059, 10297)',), overwrite=True, logger=logger)
    # proc_tokyo(db)
    process_geocode_data(db, refine=True, extra_condition=('(brand_id in (10400, 10300, 10510, 10059, 10297))',),
                         db_local=db)
    # update_city_info(db, extra_condition=('(region_e="taiwan")',), overwrite=False)
    # update_city_info(db)


    # set_stores_city(db, extra_condition=('(country_e="CHINA")',))
    # update_geo_shift(db, overwrite=False, extra_condition=('(geo_country="CHINA" AND flag=1)',))

    # import_manual_records(db, u'10237-Maison Martin Margiela.csv')
    # func(db)

    db.close()
    # db_local.close()
