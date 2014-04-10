# coding=utf-8
import ConfigParser
import datetime
import json
import os
from subprocess import check_output
import sys
import pkgutil
import scrapper.spiders
import inspect
import imp

__author__ = 'Zephyre'


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


def __fetch_brand_info():
    import core

    with core.RoseVisionDb(getattr(sys.modules[__name__], 'DB_SPEC')) as db:
        tmp = db.query('SELECT * FROM brand_info').fetch_row(how=1, maxrows=0)
        return {int(k['brand_id']): {'brandname_e': k['brandname_e'].decode('utf-8') if k['brandname_e'] else None,
                                     'brandname_c': k['brandname_c'].decode('utf-8') if k['brandname_c'] else None,
                                     'brandname_s': k['brandname_s'].decode('utf-8') if k['brandname_s'] else None}
                for k in tmp}


@static_var('currency_info', None)
def fetch_currency_info():
    info = getattr(fetch_currency_info, 'currency_info')
    if info:
        return info

    import core

    with core.RoseVisionDb(getattr(sys.modules[__name__], 'DB_SPEC')) as db:
        info = {tmp[0]: float(tmp[1]) for tmp in
                db.query('SELECT currency, rate FROM currency_info').fetch_row(maxrows=0)}
        setattr(fetch_currency_info, 'currency_info', info)
        return info


def __fetch_region_info():
    import core

    with core.RoseVisionDb(getattr(sys.modules[__name__], 'DB_SPEC')) as db:
        return {k['iso_code']: {'iso_code3': k['iso_code3'], 'status': int(k['status']),
                                'weight': int(k['weight']), 'rate': float(k['rate']),
                                'name_e': k['name_e'].decode('utf-8'),
                                'name_c': k['name_c'].decode('utf-8') if k['name_c'] else None,
                                'currency': k['currency']}
                for k in db.query('SELECT * FROM region_info').fetch_row(how=1, maxrows=0)}


__cached_region_info = None
__cached_brand_info = None
__cached_currency_rate = None
cached_spider_info = None


def region_info():
    global __cached_region_info
    if not __cached_region_info:
        __cached_region_info = __fetch_region_info()
    return __cached_region_info


def fetch_spider_info():
    from scrapper.spiders.mfashion_spider import MFashionSpider

    info = {}
    for importer, modname, ispkg in pkgutil.iter_modules(scrapper.spiders.__path__):
        f, filename, description = imp.find_module(modname, scrapper.spiders.__path__)
        try:
            submodule_list = imp.load_module(modname, f, filename, description)
        except ImportError:
            continue
        finally:
            f.close()

        sc_list = filter(
            lambda val: isinstance(val[1], type) and issubclass(val[1], MFashionSpider) and val[1] != MFashionSpider,
            inspect.getmembers(submodule_list))
        if not sc_list:
            continue
        sc_name, sc_class = sc_list[0]
        if 'brand_id' in sc_class.spider_data:
            brand_id = sc_class.spider_data['brand_id']
            info[brand_id] = sc_class

    return info


def spider_info():
    global cached_spider_info
    if not cached_spider_info:
        cached_spider_info = fetch_spider_info()
    return cached_spider_info


def brand_info():
    global __cached_brand_info
    if not __cached_brand_info:
        __cached_brand_info = __fetch_brand_info()
    return __cached_brand_info


def currency_info():
    """
    得到货币的汇率信息
    @return:
    """
    rate_data = {}
    for code, data in region_info().items():
        if data['currency'] in rate_data:
            continue
        rate_data[data['currency']] = data['rate']
    return rate_data


def _load_user_cfg(cfg_file=None, expire=600):
    """
    功能：加载配置文件。

    描述：在加载主配置文件cfg_file后，如果里面有IMPORT区域，则按照该区域的指示，加载子配置。需要注意的是expire参数。
    在加载IMPORT区域的时候，如果子配置文件已经存在，并且创建时间距离现在没有超过expire所指定的过期时间，则可以直接从本地读取子配置文件，
    不再需要根据IMPORT指示从中央服务器获取。

    @param cfg_file: 主配置文件。
    @param expire: 过期时间，单位为秒。
    @return:
    """

    def parse_val(val):
        """
        解析字符串val。如果是true/false，返回bool值；如果为整数，返回int值；如果为浮点数，返回float值。
        @param val:
        """
        if val.lower() == 'true':
            return True
        elif val.lower() == 'false':
            return False

        try:
            num = float(val)
            # 判断是浮点数还是整数
            if num == int(num):
                return int(num)
            else:
                return num
        except ValueError:
            pass

        try:
            # val为字符串，尝试是否可以解析为JSON
            return json.loads(val)
        except ValueError:
            pass

        # 尝试解析为日期字符串
        for ts_format in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y'):
            try:
                return datetime.datetime.strptime(val, ts_format)
            except ValueError:
                pass

        # 作为原始字符串返回
        return val

    def read_section(section):
        return {option: parse_val(config.get(section, option)) for option in config.options(section)}

    if not cfg_file:
        cfg_file = os.path.join(os.path.split(__file__)[0], 'mstore.cfg')

    # 加载mstore.cfg的设置内容
    config = ConfigParser.ConfigParser()
    config.optionxform = str

    try:
        with open(cfg_file, 'r') as cf:
            config.readfp(cf)
    except IOError:
        pass

    # 尝试读取远程配置文件
    if 'IMPORT' in config.sections():
        for imp_spec in sorted(read_section('IMPORT').values(),
                               key=lambda val: val['priority'] if 'priority' in val else 0):
            # 检查本地缓存文件，并决定是否采用。

            path = imp_spec['path']
            sub_cfg_file = os.path.split(path)[-1]
            if not (sub_cfg_file in os.listdir('.') and (datetime.datetime.now() - datetime.datetime.fromtimestamp(
                    os.path.getmtime(sub_cfg_file))).total_seconds() < expire):
                host = imp_spec['host'] if 'host' in imp_spec else '127.0.0.1'
                port = imp_spec['port'] if 'port' in imp_spec else 22
                username = imp_spec['username']
                head = 'pscp' if sys.platform in ('win32',) else 'scp'
                cmd_str = str.format('{4} -P {0} {1}@{2}:{3} {5}', port, username, host, path, head, sub_cfg_file)
                check_output(cmd_str, shell=True)

            _load_user_cfg(cfg_file=sub_cfg_file)

    section_list = filter(lambda val: val != 'IMPORT', config.sections())
    data = dict(map(lambda x, y: (x, y), section_list, map(read_section, section_list)))
    self_module = sys.modules[__name__]
    for key, value in data.items():
        setattr(self_module, key, value)

        # TODO 需要优化。目标：去掉这些手工指定的section。
        # conv_int = lambda val: int(val)
        # conv_bool = lambda val: val.lower() == 'true'
        #
        # # SECTION: DEBUG
        # section = 'DEBUG'
        # read_settings(section, 'DEBUG_HOST')
        # read_settings(section, 'DEBUG_PORT', proc=conv_int)
        # read_settings(section, 'DEBUG_FLAG', proc=conv_bool)
        # read_settings(section, 'LOG_DEBUG', proc=conv_bool)
        # read_settings(section, 'COOKIES_DEBUG', proc=conv_bool)
        # read_settings(section, 'COOKIES_ENABLED', proc=conv_bool)
        #
        # # SECTION: MISC
        # section = 'MISC'
        # read_settings(section, 'EMAIL_ADDR', proc=lambda val: json.loads(val))
        #
        # # SECTION DATABASE
        # section = 'DATABASE'
        # read_settings(section, 'WRITE_DATABASE', proc=conv_bool)
        # read_settings(section, 'DB_SPEC', proc=lambda val: json.loads(val))
        #
        # # SECTION STORAGE
        # section = 'STORAGE'
        # read_settings(section, 'STORAGE_PATH')
        # read_settings(section, 'HOME_PATH')
        #
        # # SECTION CHECKPOINT
        # section = 'CHECKPOINT'
        # read_settings(section, 'LAST_CRAWLED', proc=conv_datetime)
        # read_settings(section, 'LAST_PROCESS_TAGS', proc=conv_datetime)


# 切换工作目录
os.chdir(os.path.split(sys.modules[__name__].__file__)[0])
_load_user_cfg()
