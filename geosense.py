# coding=utf-8
import hashlib
import json
import re
import sys
import common as cm

__author__ = 'Zephyre'

continent_map = None
country_map = None
city_map = None
province_map = None

import random


def deref_guid(entry):
    """
    以递归的方式，替换掉guid部分
    :param entry:
    :return:
    """
    entry = dict(entry)
    if 'province' in entry:
        guid = entry['province']
        if guid != '':
            entry['province'] = deref_guid(province_map['data'][guid])

    if 'country' in entry:
        guid = entry['country']
        if guid != '':
            entry['country'] = deref_guid(country_map['data'][guid])

    if 'continent' in entry:
        guid = entry['continent']
        if guid != '':
            entry['continent'] = deref_guid(continent_map['data'][guid])

    return entry


def look_up(term, level=0):
    """
    查询一个地理词条的信息
    :param term:
    :param level: 查询级别：0：洲；1：国家；2：州/省；3：城市
    :return:
    """
    term = term.strip().upper()
    g_map = [continent_map, country_map, province_map, city_map]
    if term in g_map[level]['lookup']:
        guid = g_map[level]['lookup'][term]
        if isinstance(guid, list):
            guid = guid[0]
        entry = deref_guid(g_map[level]['data'][guid])
        return entry
    else:
        return None


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

        # 处理一下

        # with open('up1.txt', 'r') as f:
        #     for l in f.readlines():
        #         terms = l.decode('utf-8').split(',')
        #         state = terms[1].strip().upper()
        #         name_c, name_e = terms[-2].strip().upper(), terms[-1].strip().upper()
        #         ret = look_up(name_e, 3)
        #         if ret is not None:
        #             entry = ret[0]
        #             entry['name_c'] = name_c
        #             ret_p = look_up(state, 2)
        #             if ret_p is None:
        #                 print 'Cannot find %s' % state
        #             else:
        #                 entry['province'] = province_map['lookup'][state]
        #             print 'Updated: %s' % entry['name_e']
        #         else:
        #             entry = {'name_e': name_e, 'name_c': name_c}
        #             entry['province'] = province_map['lookup'][state]
        #             entry['country'] = country_map['lookup']['UNITED STATES']
        #
        #             sha = hashlib.sha1()
        #             while True:
        #                 sha.update(str(random.randint(0, sys.maxint)))
        #                 guid = ''.join(['%x' % ord(v) for v in sha.digest()])
        #                 if guid not in city_map['data']:
        #                     city_map['data'][guid] = entry
        #                     city_map['lookup'][name_e] = guid
        #                     city_map['lookup'][name_c] = guid
        #                     print 'Added: %s' % entry['name_e']
        #                     break

        # with open('data/city_map_new.dat', 'w') as f:
        #     js=json.dumps(city_map, ensure_ascii=False)
        #     f.write(js.encode('utf-8'))


        # new_map = {'data': {}, 'lookup': {}}
        # sha = hashlib.sha1()
        # for key in city_map:
        #     val = city_map[key]
        #     if val['city_e'] in new_map['lookup']:
        #         new_map['lookup'][key] = new_map['lookup'][val['city_e']]
        #         continue
        #     while True:
        #         sha.update(str(random.randint(0, sys.maxint)))
        #         guid = ''.join(['%x' % ord(v) for v in sha.digest()])
        #         if guid not in new_map['data']:
        #             tmpdict = {'name_e': val['city_e'], 'name_c': '', 'province': '',
        #                        'country': country_map['lookup'][val['country']]}
        #             if 'city_c' in val:
        #                 tmpdict['name_c'] = val['city_c']
        #             if 'province' in val and val['province'] != '':
        #                 tmpdict['province'] = province_map['lookup'][val['province']]
        #             new_map['data'][guid] = tmpdict
        #             new_map['lookup'][val['city_e']] = [guid]
        #             new_map['lookup'][key] = [guid]
        #             if 'city_c' in val and val['city_c'] != '':
        #                 # new_map['lookup'][val['city_c']].add(guid)
        #                 new_map['lookup'][val['city_c']] = [guid]
        #             break
        #
        # with open('data/city_map_new.dat', 'w') as f:
        #     f.write(json.dumps(new_map, ensure_ascii=False).encode('utf-8'))
        # x = 2


load_geo()


def field_sense(entry):
    # Geo
    country = entry[cm.country_e]
    city = entry[cm.city_e]
    ret = look_up(city, 3)
    if ret is not None:
        entry[cm.city_e] = ret['name_e']
        entry[cm.city_c] = ret['name_c']

        prov = ret['province']
        if prov != '':
            ret1 = look_up(prov['name_e'], 2)
            if ret1 is not None:
                entry[cm.province_e] = ret1['name_e']
                entry[cm.province_c] = ret1['name_c']

    ret = look_up(country, 1)
    if ret is not None:
        cm.update_entry(entry, {cm.country_e: ret['name_e'], cm.country_c: ret['name_c']})
        ret1 = look_up(ret['continent']['name_e'], 0)
        cm.update_entry(entry, {cm.continent_e: ret1['name_e'], cm.continent_c: ret1['name_c']})
    else:
        print 'Error in looking up %s' % country
    cm.chn_check(entry)


