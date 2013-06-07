# coding=utf-8
import StringIO
import gzip
import json
import re
import socket
import string
import urllib
import urllib2
import _mysql
import time

__author__ = 'Zephyre'

timeout = 15

# 字段名
continent_c = 'continent_c'
continent_e = 'continent_e'
country_c = 'country_c'
country_e = 'country_e'
province_c = 'province_c'
province_e = 'province_e'
city_c = 'city_c'
city_e = 'city_e'
name_c = 'name_c'
name_e = 'name_e'
name_l = 'name_l'
addr_c = 'addr_c'
addr_e = 'addr_e'
addr_l = 'addr_l'
hours = 'hours'
store_type = 'store_type'
url = 'url'
lng = 'lng'
lat = 'lat'
fax = 'fax'
tel = 'tel'
email = 'email'
hotline = 'hotline'
fetch_time = 'fetch_time'
zip_code = 'zip'
brand_id = 'brand_id'
brandname_e = 'brandname_e'
brandname_c = 'brandname_c'
district_c = 'district_c'
district_e = 'district_e'
comments = 'comments'

# 中日韩Unicode字符区
ucjk = ur'\u2E80-\u9FFF'
# 全角空格
ucjk_whitespace = ur'\u3000'


def chn_check(entry):
    # 检查是否为台湾、香港和澳门
    cc = entry[country_c]
    ce = entry[country_e]
    flag = False
    if u'台灣' in cc or u'台湾' in cc or u'TAIWAN' in ce:
        entry[province_c] = u'台湾'
        entry[province_e] = u'TAIWAN'
        flag = True
    elif u'香港' in cc or 'HONG KONG' in ce or 'HONGKONG' in ce:
        entry[province_c] = u'香港'
        entry[province_e] = u'HONG KONG'
        entry[city_c] = u'香港'
        entry[city_e] = u'HONG KONG'
        flag = True
    elif u'澳門' in cc or u'澳门' in cc or 'MACAU' in ce:
        entry[province_c] = u'澳门'
        entry[province_e] = u'MACAU'
        entry[city_c] = u'澳门'
        entry[city_e] = u'MACAU'
        flag = True
    if flag:
        entry[continent_c] = u'亚洲'
        entry[continent_e] = u'ASIA'
        entry[country_c] = u'中国'
        entry[country_e] = u'CHINA'
    return entry


def format_time(fmt='%Y-%m-%d %H:%M:%S'):
    """
    获得格式化的当前时间字符串
    :param fmt:
    """
    return time.strftime(fmt, time.localtime(time.time()))


def init_store_entry(id):
    """
    根据字段定义，返回一个初始化的门店结构

    """
    return {brand_id: id, fetch_time: format_time(), brandname_e: '', brandname_c: '', continent_e: '', continent_c: '',
            country_e: '', country_c: '', province_e: '', province_c: '', city_e: '', city_c: '', district_c: '',
            district_e: '',
            name_c: '', name_e: '', name_l: '', addr_e: '', addr_c: '', addr_l: '', tel: '', email: '', fax: '',
            hotline: '',
            store_type: '', hours: '', lat: '', lng: '', url: '', zip_code: ''}


def update_entry(entry, data):
    """
    更新门店数据
    :param entry:
    :param data: {'brandname_e':'foo', 'addr_e':'bar'}
    """
    for k in data.keys():
        entry[k] = data[k]


def reformat_addr(addr):
    """
    格式化地址字符串，将多余的空格、换行、制表符等合并
    """
    new_addr = html2plain(addr.strip())
    # <br/>换成换行符
    new_addr = re.subn(ur'<\s*br\s*/>', u'\r\n', new_addr)[0]
    # 去掉多余的标签
    new_addr = re.subn(ur'<.+?>', u'', new_addr)[0]
    # 换行转换
    new_addr = re.subn(ur'(?:[\r\n])+', ', ', new_addr)[0]
    new_addr = re.subn(ur'[\s\u3000]+', ' ', new_addr)[0]
    new_addr = re.subn(ur'\s+,', u',', new_addr)[0]
    new_addr = re.subn(ur',\s+', u',', new_addr)[0]
    new_addr = re.subn(ur',+', u',', new_addr)[0]
    # 去除首尾,
    new_addr = re.subn(ur'^,', ur'', new_addr)[0]
    new_addr = re.subn(ur',$', ur'', new_addr)[0]
    new_addr = re.subn(ur',', ur', ', new_addr)[0]
    return new_addr


def is_chinese(text):
    """
    是否为中文
    :param text:
    """
    return len(re.findall(ur'[\u2e80-\u9fff]+', text)) != 0


rev_char_loaded = False


def html2plain(text):
    """
    消除诸如&amp;等符号
    ("'", '&#39;'),
    ('"', '&quot;'),
    ('>', '&gt;'),
    ('<', '&lt;'),
    ('&', '&amp;')
    """
    global rev_char_loaded
    if not rev_char_loaded:
        load_rev_char()
        rev_char_loaded = True

    for k in rev_char.keys():
        pat = re.compile(k, re.I)
        text = pat.sub(rev_char[k], text)
    return text


def decode_data(data):
    """
    解码(gzip)
    """
    data = StringIO.StringIO(data)
    gzipper = gzip.GzipFile(fileobj=data)
    return gzipper.read()


def proc_response(response):
    """
    对response作处理，比如gzip，决定编码信息等
    """
    hd = response.headers.dict
    charset = 'utf-8'
    if 'content-type' in hd:
        desc = hd['content-type']
        m = re.findall('charset=(.*)', desc)
        if m.__len__() > 0:
            charset = m[0].lower()

    data = response.read()
    if 'content-encoding' in hd and hd['content-encoding'].lower().__eq__('gzip'):
        html = decode_data(data)
    else:
        html = data

    if charset in {'gb2312', 'gb18030', 'gbk'}:
        html = html.decode('gb18030')
    elif charset.__eq__('big5'):
        html = html.decode('big5')
    else:
        html = html.decode('utf-8')
    return html


def get_data(url, data=None, timeout=timeout, retry=3):
    """
    GET指定url的
    """
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"), ('Accept-Encoding', 'gzip,deflate,sdch'),
                         ('Accept-Language', 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2'),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    i = -1
    while True:
        i += 1
        try:
            if data is not None:
                response = opener.open(url, urllib.urlencode(data), timeout=timeout)
            else:
                response = opener.open(url, timeout=timeout)

            return proc_response(response)
        except Exception, e:
            if isinstance(e, urllib2.HTTPError):
                print 'http error: {0}'.format(e.code)
            elif isinstance(e, urllib2.URLError) and isinstance(e.reason, socket.timeout):
                print 'url error: socket timeout {0}'.format(e.__str__())
            else:
                print 'misc error: ' + e.__str__()
            if i >= retry:
                raise e
            else:
                continue


def dump(data):
    """
    发生错误，在日志中记录
    :param data:
    """
    f = open('err_log.txt', 'a+')
    f.write(json.dumps(data) + '\n')
    f.close()


def extract_tel(text):
    """
    是否为电话号码
    :rtype : 电话号码。如果不是，则为''
    :param text:
    """
    pat_tel = ur'[+ \.\d\-\(\)]{5,}' # ur'[+ \d\-]*\d{3,}[+ \d\-]*+'
    m_tel = re.findall(pat_tel, text)
    if len(m_tel) > 0:
        # 数字至少为6个：
        if len(re.findall(ur'\d', m_tel[0])) >= 6:
            return m_tel[0].strip()

    return ''


def extract_closure(html, start_tag, end_tag):
    """
    解析HTML标签，将对应的标签之间的内容取出来。
    :param html:
    :param start_tag:
    :param end_tag:
    :return:
    """
    # it=re.finditer(start_tag, html)
    # try:
    #     m=it.next()
    #     start=m.start()+len(m.group())
    # except StopIteration:
    #     return ''

    it = re.finditer(ur'(%s|%s)' % (start_tag, end_tag), html)
    cnt = 0
    start = 0
    end = 0
    for m in it:
        term = m.group()
        if re.match(start_tag, term):
            if cnt == 0:
                start = m.start()
            cnt += 1
        else:
            cnt -= 1
        end = m.end()
        if cnt < 0:
            return ['', 0, 0]
        elif cnt == 0:
            break
    return [html[start:end], start, end]


def post_data(url, data=None, timeout=timeout, retry=3):
    """
    POST指定url
    """

    headers = [("User-Agent",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                "Chrome/27.0.1453.94 Safari/537.36"), ('Accept-Encoding', 'gzip,deflate,sdch'),
               ('Accept-Language', 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2'),
               ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]

    i = -1
    while True:
        i += 1
        try:
            req = urllib2.Request(url)
            if data is not None:
                req.add_data(urllib.urlencode(data))
            for pair in headers:
                req.add_header(pair[0], pair[1])
            response = urllib2.urlopen(req, timeout=timeout)

            return proc_response(response)
        except Exception, e:
            if isinstance(e, urllib2.HTTPError):
                print 'http error: {0}'.format(e.code)
            elif isinstance(e, urllib2.URLError) and isinstance(e.reason, socket.timeout):
                print 'url error: socket timeout {0}'.format(e.__str__())
            else:
                print 'misc error: ' + e.__str__()
            if i >= retry:
                raise e
            else:
                continue


class StoresDb(object):
    def StoresDb(self):
        self.__brand_store_db = None

    def connect_db(self, host='localhost', user='root', passwd='', db='brand_stores'):
        """
        Connect to the brand store database
        """
        self.__brand_store_db = _mysql.connect(host=host, user=user, passwd=passwd, db=db)
        self.__brand_store_db.query("SET NAMES 'utf8'")

    def disconnect_db(self):
        """
        Disconnect the database
        """
        if self.__brand_store_db is not None:
            self.__brand_store_db.close()

    def execute(self, statement):
        try:
            if isinstance(statement, unicode):
                statement = statement.encode('utf-8')
            self.__brand_store_db.query(statement)
        except Exception, e:
            print e.__str__()

    def insert_record(self, entry, tbl):
        # INSERT INTO tbl (...) VALUES (...)
        fields = '(' + ', '.join(entry.keys()) + ')'
        # values = '(' + ', '.join([u'"' + str(entry[k]) + '"' for k in entry.keys()]) + ')'
        def get_value_term(key, value):
            if key == 'lng' or key == 'lat':
                if value == '':
                    ret = u'null'
                else:
                    ret = u'"%f"' % value
            else:
                ret = u'"%s"' % value
            return ret

        values = '(' + ', '.join([get_value_term(k, entry[k]) for k in entry.keys()]) + ')'
        statement = u'INSERT INTO %s %s VALUES %s' % (tbl, fields, values)
        self.execute(statement)

    def query(self, statement):
        """
        :param statement:
        :return: number of rows in the record set.
        """
        try:
            if isinstance(statement, unicode):
                statement = statement.encode('utf-8')
            self.__brand_store_db.query(statement)
            self.__record_set = self.__brand_store_db.store_results()
            return self.__record_set.num_rows()
        except Exception, e:
            print e.__str__()
            return 0

    def fetch_record(self):
        """
        Fetch records once a time.
        :return:
        """
        return self.__record_set.fetch_row()

    def query_all(self, statement):
        """
        Query and return all the records immediately.
        :param statement:
        :return: all the records.
        """
        if isinstance(statement, unicode):
            statement = statement.encode('utf-8')
        self.__brand_store_db.query(statement)
        self.__record_set = self.__brand_store_db.store_result()
        recordset = []
        for i in xrange(self.__record_set.num_rows()):
            recordset.append(self.__record_set.fetch_row())
        return recordset


rev_char = {}


def load_rev_char():
    f = open('html_resv.dat', 'r')
    for line in f:
        l = line.decode('utf-8')
        mlist = re.findall(ur'(([^\s]| )+)\t', l)
        rev_char[mlist[2][0]] = mlist[0][0]


country_map_c = {}
country_map_e = {}
continent_map_c = {}
continent_map_e = {}


def load_geo():
    """
    加载地理信息

    """
    f = open('geo_info.dat', 'r')
    continent_c = u'欧洲'
    continent_e = u'EUROPE'
    for line in f:
        l = line.decode('utf-8').strip()
        m = re.match(ur'(.*)国家列表', l)
        if m is not None:
            continent_c = m.group(1)
            continent_e = l.split(',')[1].strip().upper()
            continent_map_c[continent_c] = continent_e
            continent_map_e[continent_e] = continent_c
        else:
            terms = l.split(',')
            country_c = terms[0].strip()
            country_e = terms[1].strip().upper()
            if country_e not in country_map_e and country_c not in country_map_c:
                # 新的国家
                item = {'country_e': country_e, 'country_c': country_c, 'continent_e': continent_e,
                        'continent_c': continent_c}
                country_map_c[country_c] = item
                country_map_e[country_e] = item
            else:
                if country_e in country_map_e:
                    item = country_map_e[country_e]
                else:
                    item = country_map_c[country_c]
                country_map_c[country_c] = item
                country_map_e[country_e] = item


def geo_translate(c):
    """
    得到中英文对照的国家名称
    :rtype : [english, 中文]
    :param c:
    """
    if len(country_map_e) == 0 or len(country_map_c) == 0:
        load_geo()

    result = {}
    c = c.strip()
    if re.match(ur'.*[\u2E80-\u9FFF]', c) is not None:
        if country_map_c.has_key(c):
            result = country_map_c[c]
        elif continent_map_c.has_key(c):
            result = continent_map_c[c]
    else:
        c = c.upper()
        if country_map_e.has_key(c):
            result = country_map_e[c]
        elif continent_map_e.has_key(c):
            result = continent_map_e[c]
    return result


def walk_tree(node):
    """
    从根节点出发，遍历
    """
    if node['func'] == None:
        # leaf-node, return a tuple which has only one element - the data bundle itself.
        return [node['data']]

    leaf_list = []
    for entry in node['func'](node['data']):
        leaf_list.extend(walk_tree(entry))
    return leaf_list