# coding=utf-8
import json
import re
import common as cm

__author__ = 'Zephyre'

continent_map = None
country_map = None
city_map = None
province_map = None


def load_geo():
    global continent_map
    global country_map
    global city_map
    global province_map

    with open('data/continent_map.dat', 'r') as f:
        continent_map = json.load(f)
    with open('data/country_map.dat', 'r') as f:
        country_map = json.load(f)
    with open('data/province_map.dat', 'r') as f:
        province_map = json.load(f)
    with open('data/city_map.dat', 'r') as f:
        city_map = json.load(f)


load_geo()


def add_entries(continent_map={}, country_map={}, prov_map={}, city_map={}):
    with open('up.txt', 'r') as f:
        for l in f.readlines():
            splits=re.split(ur',',l)
            city=splits[0].strip().upper().decode('utf-8')
            country=splits[1].strip().upper().decode('utf-8')

            ret=look_up(city,3)
            if ret is not None:
                # exist
                continue

            ret=look_up(country,1)
            if ret is None:
                print 'Error in geosensing %s'%country
                continue

            city_map[city] = {'city_e':city, 'country':country}
            print 'Added: %s in %s'%(city, country)

            # m = re.findall(ur'(\S+)\s+?([^\s,]+)', l)
            # if len(m) > 0:
            #     # state_code = m[0][0]
            #     prov_e = m[0][0].strip().upper().decode('utf-8')
            #     city_e = m[0][1].strip().upper().decode('utf-8')
            #     print u'%s, %s' % (prov_e, city_e)
            #     if prov_e not in prov_map:
            #         prov_item = {'province_e': prov_e, 'country': country}
            #         prov_map[prov_e] = prov_item
            #     if city_e not in city_map:
            #         city_item = {'city_e': city_e, 'province': prov_e, 'country': country}
            #         city_map[city_e] = city_item
    # return

    # with open('data/province_map.dat', 'w') as f:
    #     if len(prov_map) > 0:
    #         js = json.dumps(prov_map, ensure_ascii=False)
    #         f.write(js.encode('utf-8'))

    with open('data/city_map.dat', 'w') as f:
        if len(city_map) > 0:
            js = json.dumps(city_map, ensure_ascii=False)
            f.write(js.encode('utf-8'))

    # with open('data/country_map.dat', 'w') as f:
    #     if len(country_map) > 0:
    #         js = json.dumps(country_map, ensure_ascii=False)
    #         f.write(js.encode('utf-8'))
    #
    # with open('data/continent_map.dat', 'w') as f:
    #     if len(continent_map) > 0:
    #         js = json.dumps(continent_map, ensure_ascii=False)
    #         f.write(js.encode('utf-8'))

def field_sense(entry):
    # Geo
    country = entry[cm.country_e]
    city = entry[cm.city_e]
    ret = look_up(city, 3)
    if ret is not None:
        if cm.city_e in ret[0]:
            entry[cm.city_e] = ret[0][cm.city_e]
        if cm.city_c in ret[0]:
            entry[cm.city_c] = ret[0][cm.city_c]
        if 'province' in ret[0]:
            ret1 = look_up(ret[0]['province'], 2)
            if ret1 is not None:
                ret1 = ret1[0]
                if cm.province_e in ret1:
                    entry[cm.province_e] = ret1[cm.province_e]
                if cm.province_c in ret1:
                    entry[cm.province_c] = ret1[cm.province_c]
    ret = look_up(country, 1)
    if ret is not None:
        cm.update_entry(entry, {cm.country_e: ret[0][cm.country_e], cm.country_c: ret[0][cm.country_c]})
        ret1 = look_up(ret[0]['continent'], 0)[0]
        cm.update_entry(entry, {cm.continent_e: ret1[cm.continent_e], cm.continent_c: ret1[cm.continent_c]})
    else:
        print 'Error in looking up %s' % country
    cm.chn_check(entry)

def look_up(term, level=-1):
    """
    查询一个地理词条的信息
    :param term:
    :param level: 查询级别：0：洲；1：国家；2：州/省；3：城市
    :return:
    """
    term = term.strip().upper()
    g_map = [continent_map, country_map, province_map, city_map]
    if level == -1:
        for i in xrange(4):
            ret = look_up(term, i)
            if ret is not None:
                return ret
    else:
        if term in g_map[level]:
            return g_map[level][term], level
        else:
            return None