# coding=utf-8
import json
import string
import re
import common as cm

__author__ = 'Zephyre'

db = None
url = 'http://www.zenith-watches.com/zh_zh/shoplocator.html'
brand_id = 10395
brandname_e = u'Zenith'
brandname_c = u'真力时'


def fetch_store_list(url):
    """
    获得门店的列表
    :rtype : 门店列表。格式：[{'name':**, 'lat':**, 'lng':**, 'type':**, 'url':**}]
    :param url: 
    """
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'data': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    # 开始解析工作
    # 查找数据部分，位于var items和var\s\w+之间
    start = html.find('var items')
    if start == -1:
        return {}
    start += len('var items')
    end = html.find('var ', start)
    html = html[start:end]
    stores = []

    pattern = ur'\[(.+?)\]'
    store_list = []
    for m in re.findall(pattern, html, re.S):
        store_entry = {}
        m_list = re.findall(ur"'(.*)'", m)
        try:
            store_entry['name'] = cm.html2plain(m_list[0].strip())
            store_entry['type'] = m_list[2].strip()
            store_entry['url'] = m_list[4].strip()
        except IndexError:
            print 'Index error: %s' % m
            # 去掉引号之间的内容，准备查找经纬度信息
        m_list = re.findall(ur'(-?\d+\.\d+)', re.subn(ur"'(.*)'", '', m)[0])
        try:
            lat = string.atof(m_list[0])
            lng = string.atof(m_list[1])
            store_entry['lat'] = lat
            store_entry['lng'] = lng
        except (IndexError, ValueError):
            print 'Index error in getting coordinates: %s' % m

        # test
        # if 'hong-kong' in store_entry['url'] or 'taichung' in store_entry['url']:
        if len(store_entry.keys()) > 0:
            store_list.append(store_entry)
    return store_list


def fetch_store_details(url, data):
    """
    获得门店的详细信息（url下可能有多个门店）
    :rtype : [{}]
    :param url:
    :param data:
    """
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s / %s' % (str(data), url)
        dump_data = {'level': 2, 'time': cm.format_time(), 'data': data, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    # 可能有多个门店，拆分
    sub_html = []
    for m in re.finditer(ur'<li\s+class\s*=\s*"boutique-info-cadre-\d+"\s*>', html):
        start = m.start() + len(m.group())
        end = html.find('</li>', start)
        sub_html.append(html[start:end])

    stores = []
    # 针对每个门店：
    for s in sub_html:
        entry = cm.init_store_entry(brand_id)
        cm.update_entry(entry, {cm.brandname_c: brandname_c, cm.brandname_e: brandname_e, cm.url: url,
                                cm.name_e: data['name'], cm.lat: data['lat'], cm.lng: data['lng'],
                                cm.store_type: data['type']})
        for m in re.findall(ur'<p class="boutique-info-cadre-titre">(.*?)</p>', s):
            if len(m.strip()) >= 0:
                entry[cm.store_type] = m.strip()
            break
        for m in re.findall(ur'<p class="boutique-info-cadre-tel">(.*)</p>', s, re.S):
            if len(m.strip()) == 0:
                break
            for m1 in re.findall(ur'<span itemprop="telephone">(.*?)</span>', m):
                if len(m1.strip()) > 0:
                    entry[cm.tel] = m1.strip()
                break
            for m1 in re.findall(ur'<span itemprop="faxNumber">(.*?)</span>', m):
                if len(m1.strip()) > 0:
                    entry[cm.fax] = m1.strip()
                break
            if entry[cm.tel] == '' and entry[cm.fax] == '':
                entry[cm.tel] = cm.extract_tel(m.strip())
        for m in re.findall(ur'<p class="boutique-info-cadre-horaires">(.*?)</p>', s, re.S):
            if len(m.strip()) > 0:
                entry[cm.hours] = m.strip()
            break
        for m in re.findall(ur'<p class="boutique-info-cadre-adresse".*?>(.*?)</p>', s, re.S):
            if len(m.strip()) == 0:
                break
            street_addr = ''
            zip_code = ''
            city = ''
            country = ''
            for m1 in re.findall(ur'<span itemprop="streetAddress">(.*?)</span>', m, re.S):
                if len(m1.strip()) > 0:
                    street_addr = cm.reformat_addr(m1)
                break
            for m1 in re.findall(ur'<span itemprop="postalCode">(.*?)</span>', m):
                if len(m1.strip()) > 0:
                    zip_code = m1
                break
            for m1 in re.findall(ur'<span itemprop="addressLocality">(.*?)</span>', m):
                if len(m1.strip()) > 0:
                    city = m1
                break
            for m1 in re.findall(ur'<span itemprop="addressCountry">(.*?)</span>', m):
                if len(m1.strip()) > 0:
                    country = m1
                break
            entry[cm.zip_code] = zip_code
            # 没有上述标签的情况
            if street_addr == '':
                tmp = cm.reformat_addr(m)
                terms = tmp.split(',')
                t2 = cm.geo_translate(terms[-1])
                if len(t2) != 0:
                    # 这是一个国家
                    # 把最后的国家项分离出来
                    street_addr = ', '.join(terms[:-1])
                    entry[cm.addr_e] = cm.reformat_addr(street_addr)
                    entry[cm.country_c] = t2['country_c']
                    entry[cm.country_e] = t2['country_e']
                    entry[cm.continent_c] = t2['continent_c']
                    entry[cm.continent_e] = t2['continent_e']
                else:
                    if cm.is_chinese(tmp):
                        entry[cm.addr_c] = tmp
                    else:
                        entry[cm.addr_e] = tmp
            else:
                street_addr = ', '.join([street_addr, zip_code, city])
                entry[cm.addr_e] = cm.reformat_addr(street_addr)
                t2 = cm.geo_translate(country)
                if len(t2) == 0:
                    entry[cm.country_c] = country
                else:
                    entry[cm.country_c] = t2['country_c']
                    entry[cm.country_e] = t2['country_e']
                    entry[cm.continent_c] = t2['continent_c']
                    entry[cm.continent_e] = t2['continent_e']
                entry[cm.city_e] = city
        print '%s Found store: %s, %s. (%s, %s)' % (brandname_e,
                                                    entry[cm.name_e], entry[cm.addr_e], entry[cm.continent_e],
                                                    entry[cm.country_e])
        cm.chn_check(entry)
        stores.append(entry)
        db.insert_record(entry, 'stores')
    return stores


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """

        :param data:
        :param level: 1: 获得众多门店列表；2：获得单独的门店信息
        """
        if level == 1:
            store_list = fetch_store_list(data['url'])
            return [{'func': lambda data: func(data, 2), 'data': s} for s in store_list]
        elif level == 2:
            stores = fetch_store_details(data['url'], data)
            return [{'func': None, 'data': s} for s in stores]
        pass

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = cm.walk_tree({'func': lambda data: func(data, 1), 'data': data})
    db.disconnect_db()
    return results