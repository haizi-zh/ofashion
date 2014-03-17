# coding=utf-8
import ConfigParser
import datetime
import json
import os

__author__ = 'Zephyre'

import sys
import pkgutil
import scrapper.spiders
import inspect
import imp


def __fetch_brand_info():
    import core
    with core.RoseVisionDb(getattr(sys.modules[__name__], 'DB_SPEC')) as db:
        tmp = db.query('SELECT * FROM brand_info').fetch_row(how=1, maxrows=0)
        return {int(k['brand_id']): {'brandname_e': k['brandname_e'].decode('utf-8') if k['brandname_e'] else None,
                                     'brandname_c': k['brandname_c'].decode('utf-8') if k['brandname_c'] else None,
                                     'brandname_s': k['brandname_s'].decode('utf-8') if k['brandname_s'] else None}
                for k in tmp}


def __fetch_region_info():
    import core
    with core.RoseVisionDb(getattr(sys.modules[__name__], 'DB_SPEC')) as db:
        tmp = db.query('SELECT * FROM region_info').fetch_row(how=1, maxrows=0)
        return {k['iso_code']: {'iso_code3': k['iso_code3'],
                                'weight': int(k['weight']), 'rate': float(k['rate']),
                                'name_e': k['name_e'].decode('utf-8'),
                                'name_c': k['name_c'].decode('utf-8') if k['name_c'] else None,
                                'currency': k['currency']}
                for k in tmp}


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
        f, filename, description = imp.find_module(modname, ['scrapper/spiders'])
        try:
            submodule_list = imp.load_module(modname, f, filename, description)
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


def _load_user_cfg(cfg_file=None):
    if not cfg_file:
        cfg_file = os.path.join(os.path.split(__file__)[0], 'mstore.cfg')

    # 加载mstore.cfg的设置内容
    config = ConfigParser.ConfigParser()
    config.optionxform = str

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
        try:
            return datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(val, '%Y-%m-%d')
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(val, '%m/%d/%Y %H:%M:%S')
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(val, '%m/%d/%Y')
        except ValueError:
            pass

        # 作为原始字符串返回
        return val

    def read_section(section):
        return section, {option: parse_val(config.get(section, option)) for option in config.options(section)}

    def read_settings(section, option, var=None, proc=lambda x: x):
        """
        从config文件中指定的section，读取指定的option，并写到全局变量var中。
        @param section:
        @param option:
        @param var: 如果为None，则视为等同于option。
        @param proc: 对读取到的option，应该如何处理？
        @return:
        """
        if not var:
            var = option
        self_module = sys.modules[__name__]
        if section in config.sections() and option in config.options(section):
            setattr(self_module, var, proc(config.get(section, option)))
            return True
        else:
            return False

    conv_int = lambda val: int(val)
    conv_bool = lambda val: val.lower() == 'true'

    def conv_datetime(val):
        try:
            return datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    try:
        with open(cfg_file, 'r') as cf:
            config.readfp(cf)
    except IOError:
        pass

    data = dict(map(read_section, config.sections()))
    self_module = sys.modules[__name__]
    for key, value in data.items():
        setattr(self_module, key, value)

    # SECTION: DEBUG
    section = 'DEBUG'
    read_settings(section, 'DEBUG_HOST')
    read_settings(section, 'DEBUG_PORT', proc=conv_int)
    read_settings(section, 'DEBUG_FLAG', proc=conv_bool)
    read_settings(section, 'LOG_DEBUG', proc=conv_bool)
    read_settings(section, 'COOKIES_DEBUG', proc=conv_bool)
    read_settings(section, 'COOKIES_ENABLED', proc=conv_bool)

    # SECTION: MISC
    section = 'MISC'
    read_settings(section, 'EMAIL_ADDR', proc=lambda val: json.loads(val))

    # SECTION DATABASE
    section = 'DATABASE'
    read_settings(section, 'WRITE_DATABASE', proc=conv_bool)
    read_settings(section, 'DB_SPEC', proc=lambda val: json.loads(val))

    # SECTION STORAGE
    section = 'STORAGE'
    read_settings(section, 'STORAGE_PATH')
    read_settings(section, 'HOME_PATH')

    # SECTION CHECKPOINT
    section = 'CHECKPOINT'
    read_settings(section, 'LAST_CRAWLED', proc=conv_datetime)
    read_settings(section, 'LAST_PROCESS_TAGS', proc=conv_datetime)


_load_user_cfg()