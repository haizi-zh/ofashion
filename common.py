# coding=utf-8
import logging
import StringIO
import gzip
import json
import re
import socket
import urllib
import urllib2
import _mysql
import time
import HTMLParser
import urlparse


__author__ = 'Zephyre'

timeout = 30

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
store_class = 'store_class'
native_id = 'native_id'

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


def extract_city(city):
    zip_code = ''
    val = html2plain(city).strip().upper()
    pat = re.compile(ur'^[\d\-\. ]*')
    m = re.search(pat, val)
    if m:
        zip_code = m.group().strip()
        val = re.sub(pat, '', val)

    pat = re.compile(ur'[\d\- ]*$')
    m = re.search(pat, val)
    if m:
        zip_code = m.group().strip()
        val = re.sub(pat, '', val)

    m = re.search(ur'[\d,/]', val)
    if m:
        val = val[:m.start()]
    m = re.search(ur'\s+-\s+', val)
    if m:
        val = val[:m.start()]
    val = re.sub(ur'"*$', '', val).strip()
    val = re.sub(ur"'*$", '', val).strip()
    val = re.sub(ur'^"*', '', val).strip()
    val = re.sub(ur"^'*", '', val).strip()
    val = re.sub(ur'\([^\(\)]*\)', '', val).strip()

    return val, zip_code


def format_time(fmt='%Y-%m-%d %H:%M:%S'):
    """
    获得格式化的当前时间字符串
    :param fmt:
    """
    return time.strftime(fmt, time.localtime(time.time()))


def init_store_entry(bn_id, bn_e='', bn_c=''):
    """
    根据字段定义，返回一个初始化的门店结构

    """
    return {brand_id: bn_id, fetch_time: format_time(), brandname_e: bn_e, brandname_c: bn_c, continent_e: '',
            continent_c: '', country_e: '', country_c: '', province_e: '', province_c: '', city_e: '', city_c: '',
            district_c: '', district_e: '', name_l: '', name_e: '', name_c: '', addr_e: '', addr_c: '',
            addr_l: '', tel: '', email: '', fax: '', store_class: '', 'is_geocoded': 0, 'native_id': '',
            comments: '', hotline: '', store_type: '', hours: '', lat: '', lng: '', url: '', zip_code: ''}


def update_entry(entry, data):
    """
    更新门店数据
    :param entry:
    :param data: {'brandname_e':'foo', 'addr_e':'bar'}
    """
    for k in data.keys():
        entry[k] = data[k]


def argument_parse(text):
    arg_list = []
    start = 0
    s_opened = False
    d_opened = False
    escaped = False
    for i in xrange(len(text)):
        if text[i] == "'" and not d_opened and not escaped:
            s_opened = not s_opened
        elif text[i] == '"' and not s_opened and not escaped:
            d_opened = not d_opened
        elif text[i] == ',' and not s_opened and not d_opened and not escaped:
            arg_list.append(text[start:i].strip())
            start = i + 1

        escaped = (text[i] == '\\' and not escaped)

    last = text[start:].strip()
    if last != '':
        arg_list.append(last)

    for i in xrange(len(arg_list)):
        tmp = arg_list[i]
        arg_list[i] = re.sub(ur'\\(.)', ur'\1', tmp)
    return tuple(arg_list)


def reformat_addr(addr):
    """
    格式化地址字符串，将多余的空格、换行、制表符等合并
    """
    if addr is None:
        return None
    new_addr = html2plain(addr.strip())
    # <br/>换成换行符
    new_addr = re.subn(ur'<\s*br\s*/?>', u'\r\n', new_addr)[0]
    # 去掉多余的标签
    new_addr = re.subn(ur'<[^<>]*?>', u'', new_addr)[0]
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
    # 去掉多余引号
    val = re.sub(ur'"*$', '', new_addr).strip()
    val = re.sub(ur"'*$", '', val).strip()
    val = re.sub(ur'^"*', '', val).strip()
    val = re.sub(ur"^'*", '', val).strip()
    val = re.sub(ur'"{2,}', ' ', val).strip()
    val = re.sub(ur"'{2,}", ' ', val).strip()
    val = re.sub(ur'^\.+', '', val).strip()
    val = re.sub(ur'^\.{2,}', '.', val).strip()
    val = '' if val == '.' else val
    return val


def is_chinese(text):
    """
    是否为中文
    :param text:
    """
    return len(re.findall(ur'[\u2e80-\u9fff]+', text)) != 0


unescape_hlp = HTMLParser.HTMLParser()


def html2plain(text):
    """
    消除诸如&amp;等符号
    ("'", '&#39;'),
    ('"', '&quot;'),
    ('>', '&gt;'),
    ('<', '&lt;'),
    ('&', '&amp;')
    """
    while True:
        m = re.search(ur'&.+?;', text)
        if m is None:
            break
        text = text[:m.start()] + unescape_hlp.unescape(m.group(0).lower()) + text[m.end():]

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

    cookie_map = {}
    if 'set-cookie' in hd.keys():
        for cookie in hd['set-cookie'].split(','):
            m = re.search(r'(.+?)=(.+)[;$]', cookie)
            if m:
                cookie_map[m.group(1).strip()] = m.group(2).strip()


                # for term in re.split(';', hd['set-cookie']):
                #     if re.match('^\s*Expires=', term):
                #         continue
                #     pat = re.compile('^\s*Path=.*?[,/]+', re.I)
                #     term = re.sub(pat, '', term)
                #
                #     m = re.search('=', term)
                #     if m is not None:
                #         cookie_map[term[:m.start()].strip()] = term[m.end():].strip()

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
        # 有时候，尽管标称为utf-8，实际上无法解码
        default_charset = ['utf-8', 'iso-8859-1', 'gbk']
        idx = 0
        while True:
            try:
                html = html.decode(default_charset[idx])
                break
            except UnicodeDecodeError:
                idx += 1
                if idx >= len(default_charset):
                    break
                else:
                    continue

    # if len(cookie_map) == 0:
    #     return html, None
    # else:
    return html, cookie_map


def get_data(url, data=None, hdr=None, timeout=timeout, retry=3, cool_time=20, cookie=None, client='Desktop',
             userAgent='', logger=None, verbose=False):
    """
    Fetch data from a URL via HTTP GET method, ignoring all the cookies. See get_data_cookie.
    """
    html, cookie = get_data_cookie(url, data, hdr, timeout, retry, cool_time, cookie, client=client,
                                   userAgent=userAgent, logger=logger, verbose=verbose)
    return html


def get_data_cookie(url, data=None, hdr=None, timeout=timeout, retry=3, cool_time=20, cookie=None, proxy=None,
                    client='Desktop', userAgent='', logger=None, verbose=False):
    """
    Fetch data from a URL via HTTP GET method, with cookies provided.
    :param url:
    :param data: parameters provided in the form of key/value pairs.
    :param hdr: user defined additional HTTP headers.
    :param timeout:
    :param retry:
    :param cool_time:
    :param cookie:
    :param proxy:
    :param client: predefined clients are 'Desktop' and 'iPad', or userAgent will be used.
    :param userAgent:
    :param logger:
    :param verbose:
    :return: :raise:
    """
    if not logger:
        logger=logging.getLogger('root')
    if isinstance(url, unicode):
        url = url.encode('utf-8')
    url_components = [item for item in urlparse.urlsplit(url)]
    url_components[2] = urllib.quote(urllib.unquote(url_components[2]))

    if proxy is not None:
        proxy_handler = urllib2.ProxyHandler({'http': proxy['url']})
        opener = urllib2.build_opener(proxy_handler)
    else:
        opener = urllib2.build_opener()

    hdr_map = dict([('Accept-Encoding', 'gzip,deflate,sdch'),
                    ('Accept-Language', 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2'),
                    ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')])
    if client.lower() == 'desktop':
        hdr_map[
            "User-Agent"] = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36"
    elif client.lower() == 'ipad':
        hdr_map[
            'User-Agent'] = 'Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10'
    else:
        hdr_map['User-Agent'] = userAgent

    if hdr is not None:
        for key in hdr:
            val = hdr[key]
            hdr_map[key] = val.encode('utf-8') if isinstance(val, unicode) else val

    if cookie:
        cookie_str = '; '.join(['%s=%s' % (k, cookie[k]) for k in cookie.keys()])
        hdr_map['Cookie'] = cookie_str

    opener.addheaders = [(k, hdr_map[k]) for k in hdr_map]
    if data:
        for key in data:
            if isinstance(data[key], unicode):
                data[key] = data[key].encode('utf-8')
        url_components[3] = '&'.join((url_components[3], urllib.urlencode(data))) \
            if url_components[3] != '' else urllib.urlencode(data)

    url = urlparse.SplitResult(*url_components).geturl()

    i = -1
    while True:
        i += 1
        try:
            body, cookie_new = proc_response(opener.open(url, timeout=timeout))
            if cookie:
                for key in cookie:
                    if key not in cookie_new:
                        cookie_new[key] = cookie[key]
            return body, cookie_new
        except urllib2.HTTPError:
            pass
        except Exception, e:
            if isinstance(e, urllib2.HTTPError):
                print 'Http error: {0}'.format(e.code)
            elif isinstance(e, urllib2.URLError) and isinstance(e.reason, socket.timeout):
                print 'Url error: socket timeout {0}'.format(e.__str__())
            else:
                print 'Misc error: ' + e.__str__()
            if i >= retry:
                raise e
            else:
                print 'Cooling down for %fsec...' % cool_time
                time.sleep(cool_time)
                continue


def dump(data, filename, is_json=False, to_console=True):
    """
    在日志中记录
    :param data:
    """
    f = open(filename, 'a+')
    if is_json:
        f.write((u'%s : %s\n' % (format_time(), json.dumps(data))).encode('utf-8'))
    else:
        f.write((u'%s : %s\n' % (format_time(), data)).encode('utf-8'))
    if to_console:
        print(u'%s : %s' % (format_time(), data))
    f.close()


def extract_email(txt):
    pat_email = re.compile(
        r'(?:[a-z0-9!#$%&\'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&\'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])')
    m = re.search(pat_email, txt)
    if m is None:
        return ''
    else:
        return m.group()


def extract_tel(text):
    """
    是否为电话号码
    :rtype : 电话号码。如果不是，则为''
    :param text:
    """
    pat_tel = ur'[+ \.\d\-\(\)]{5,}' # ur'[+ \d\-]*\d{3,}[+ \d\-]*+'
    m_tel = re.findall(pat_tel, text)
    for m in m_tel:
        if len(re.findall(ur'\d', m)) >= 6:
            return m.strip()
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


def write_log(msg, log_type='Error'):
    log_name = format_time('%Y%m%d_%H%M%S') + '.log'
    timestr = format_time()
    with open(log_name, 'a') as f:
        f.write((u'%s %s %s\n' % (timestr, log_type, msg)).encode('utf-8'))


def post_data(url, data=None, hdr=None, timeout=timeout, retry=5, cookie=None):
    html, cookie = post_data_cookie(url, data, hdr, timeout, retry, cookie)
    return html


def post_data_cookie(url, data=None, hdr=None, timeout=timeout, retry=5, cookie=None, proxy=None, client='Desktop',
                     userAgent=''):
    """
    POST指定url
    """
    url = url.encode('utf-8')
    url_components = [item for item in urlparse.urlsplit(url)]
    url_components[2] = urllib.quote(urllib.unquote(url_components[2]))
    url = urlparse.SplitResult(*url_components).geturl()

    headers = [('Accept-Encoding', 'gzip,deflate,sdch'),
               ('Accept-Language', 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2'),
               ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    hdr_map = dict(headers)
    if client == 'Desktop':
        hdr_map[
            "User-Agent"] = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36"
    elif client == 'iPad':
        hdr_map[
            'User-Agent'] = 'Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10'
    else:
        hdr_map['User-Agent'] = userAgent

    if hdr is not None:
        for key in hdr:
            val = hdr[key]
            if isinstance(val, unicode):
                val = val.encode('utf-8')
            hdr_map[key] = val

    if cookie is not None:
        cookie_str = '; '.join(['%s=%s' % (k, cookie[k]) for k in cookie.keys()])
        headers.append(('Cookie', cookie_str))
        hdr_map['Cookie'] = cookie_str

    req = urllib2.Request(url)
    if data is not None:
        for key in data:
            if isinstance(data[key], unicode):
                data[key] = data[key].encode('utf-8')
        req.add_data(urllib.urlencode(data))

    for key in hdr_map:
        req.add_header(key, hdr_map[key])

    i = -1
    while True:
        i += 1
        try:
            return proc_response(urllib2.urlopen(req, timeout=timeout))
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
        self._store_db = None

    def connect_db(self, host='localhost', port=3306, user='root', passwd='', db='brand_stores', autocommit=True):
        """
        Connect to the brand store database
        """
        self._store_db = _mysql.connect(host=host, port=port, user=user, passwd=passwd, db=db)
        self._store_db.autocommit(autocommit)
        self._store_db.query("SET NAMES 'utf8'")

    def disconnect_db(self):
        """
        Disconnect the database
        """
        if self._store_db is not None:
            self._store_db.commit()
            self._store_db.close()

    def execute(self, statement):
        try:
            if isinstance(statement, unicode):
                statement = statement.encode('utf-8')
            self._store_db.query(statement)
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
                # 去掉中间的引号
                ret = u'"%s"' % unicode(value).replace('"', '\\"')
                # ret = u'"%s"' % unicode(value).replace('"', '').replace("'", '').replace('\\', '')
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
            self._store_db.query(statement)
            self._record_set = self._store_db.store_result()
            return self._record_set.num_rows()
        except Exception, e:
            print e
            return 0

    def fetch_record(self):
        """
        Fetch records once a time.
        :return:
        """
        return self._record_set.fetch_row()

    def query_all(self, statement):
        """
        Query and return all the records immediately.
        :param statement:
        :return: all the records.
        """
        if isinstance(statement, unicode):
            statement = statement.encode('utf-8')
        self._store_db.query(statement)
        self._record_set = self._store_db.store_result()
        recordset = []
        tmp = []
        for i in xrange(self._record_set.num_rows()):
            tmp.append(self._record_set.fetch_row()[0])

            # recordset.append(tuple(tmp.decode('utf-8') for tmp in ))
            # recordset.extend(self._record_set.fetch_row())
        return tuple(
            tuple(term.decode('utf-8') if term and isinstance(term, str) else term for term in item) for item in tmp)
        # return tuple(tuple(term.decode('utf-8') for term in item) for item in tmp)
        # return tuple(recordset)


# rev_char = {}


# def load_rev_char():
#     f = open('data/html_resv.dat', 'r')
#     for line in f:
#         l = line.decode('utf-8')
#         mlist = re.findall(ur'(([^\s]| )+)\t', l)
#         rev_char[mlist[2][0]] = mlist[0][0]
#         rev_char[mlist[1][0]] = mlist[0][0]
#
#
# load_rev_char()

continent_map = None
country_map = None
city_map = None
province_map = None


def get_time_str(seconds):
    """
    将秒换算成时间字符串
    :param seconds:
    """
    seconds = int(seconds)
    est_hr = seconds / 3600
    est_min = (seconds % 3600) / 60
    est_sec = (seconds % 3600) % 60
    return u'%02d:%02d:%02d' % (est_hr, est_min, est_sec)


def load_geo():
    global continent_map
    global country_map
    global city_map
    with open('geo_data.dat', 'r') as file_:
        all_map = json.loads(file_)

    continent_map = all_map['continent_map']
    country_map = all_map['country_map']
    city_map = all_map['city_map']


def load_geo2():
    """
    加载地理信息

    """
    continent_map = {}
    country_map = {}
    city_map = {}
    f = open('geo_info.dat', 'r')
    continent_c = u'欧洲'
    continent_e = u'EUROPE'
    for line in f:
        l = line.decode('utf-8').strip()
        m = re.match(ur'(.*)国家列表', l)
        if m is not None:
            continent_c = m.group(1).strip()
            continent_e = l.split(',')[1].strip().upper()
            continent_map[continent_c] = continent_e
            continent_map[continent_e] = continent_c
        else:
            terms = l.split(',')
            country_c = terms[0].strip()
            country_e = terms[1].strip().upper()
            capital_c = terms[2].strip()
            capital_e = terms[3].strip().upper()
            if country_e not in country_map and country_c not in country_map:
                # 新的国家
                item = {'country_e': country_e, 'country_c': country_c, 'continent_e': continent_e,
                        'continent_c': continent_c, 'capital_e': capital_e, 'capital_c': capital_c}
                country_map[country_c] = item
                country_map[country_e] = item
            else:
                if country_e in country_map:
                    item = country_map[country_e]
                else:
                    item = country_map[country_c]
                country_map[country_c] = item
                country_map[country_e] = item
    f.close()

    # 城市信息
    for c in country_map:
        city_c = country_map[c]['capital_c']
        city_e = country_map[c]['capital_e']
        country = country_map[c]['country_e']
        item = {'city_c': city_c, 'city_e': city_e, 'country': country}
        if city_c != '':
            city_map[city_c] = item
        if city_e != '':
            city_map[city_e] = item

    all_geo = {'continent_map': continent_map, 'country_map': country_map, 'city_map': city_map}
    jstr = json.dumps(all_geo, ensure_ascii=False)
    f = open('geo_data.dat', 'w')
    f.write(jstr.encode('utf-8'))
    f.close()

    f = open('geo_data.dat', 'r')
    jobj = json.load(f)
    f.close()


# def geo_translate(c, level=-1):
#     """
#     得到中英文对照的国家名称
#     :param level: 查找级别：-1：全级别查找：自上而下；0：洲；1：国家；2：州/省；3:城市；4：城市区域
#     :rtype : [english, 中文]
#     :param c:
#     """
#     if continent_map is None:
#         load_geo()
#
#     result = {}
#     c = c.strip()
#     if re.match(ur'.*[\u2E80-\u9FFF]', c) is not None:
#         if country_map_c.has_key(c):
#             result = country_map_c[c]
#         elif continent_map_c.has_key(c):
#             result = continent_map_c[c]
#     else:
#         c = c.upper()
#         if country_map_e.has_key(c):
#             result = country_map_e[c]
#         elif continent_map_e.has_key(c):
#             result = continent_map_e[c]
#     return result


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