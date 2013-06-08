# coding=utf-8
import json
import re

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

    with open('continent_map.dat', 'r') as f:
        continent_map = json.load(f)
    with open('country_map.dat', 'r') as f:
        country_map = json.load(f)
    with open('province_map.dat', 'r') as f:
        province_map = json.load(f)
    with open('city_map.dat', 'r') as f:
        city_map = json.load(f)


load_geo()


def add_entries(continent_map={}, country_map={}, prov_map={}, city_map={}):
    ret = look_up('CHINA')
    ret = look_up(u'美国')
    ret = look_up(u'成都')
    ret = look_up(u'四川')
    ret = look_up('CHongqing')

    js = json.dumps(prov_map, ensure_ascii=False)

    country = country_map[u'JAPAN']['country_e']
    with open('up.txt', 'r') as f:
        for l in f.readlines():
            m = re.findall(ur'(\S+)\s+?([^\s,]+)', l)
            # m = re.findall(ur'([A-Z]{2})\s*([\w ]+?)\s*-\s*([\w ]+)', l.decode('utf-8'))
            if len(m) > 0:
                # state_code = m[0][0]
                prov_e = m[0][0].strip().upper().decode('utf-8')
                city_e = m[0][1].strip().upper().decode('utf-8')
                print u'%s, %s' % (prov_e, city_e)
                if prov_e not in prov_map:
                    prov_item = {'province_e': prov_e, 'country': country}
                    prov_map[prov_e] = prov_item
                if city_e not in city_map:
                    city_item = {'city_e': city_e, 'province': prov_e, 'country': country}
                    city_map[city_e] = city_item

    js = json.dumps(prov_map, ensure_ascii=False)
    with open('province_map.dat', 'w') as f:
        if len(prov_map) > 0:
            js = json.dumps(prov_map, ensure_ascii=False)
            f.write(js.encode('utf-8'))

    with open('city_map.dat', 'w') as f:
        if len(city_map) > 0:
            js = json.dumps(city_map, ensure_ascii=False)
            f.write(js.encode('utf-8'))

    with open('country_map.dat', 'w') as f:
        if len(country_map) > 0:
            js = json.dumps(country_map, ensure_ascii=False)
            f.write(js.encode('utf-8'))

    with open('continent_map.dat', 'w') as f:
        if len(continent_map) > 0:
            js = json.dumps(continent_map, ensure_ascii=False)
            f.write(js.encode('utf-8'))


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