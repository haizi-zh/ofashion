# coding=utf-8

"""
得到一些常用的信息
"""

import inspect
import pkgutil
import imp
import datetime
import scrapper.spiders
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.db import RoseVisionDb
import global_settings

__author__ = 'Zephyre'


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


@static_var('region_info', None)
def region_info(refetch=False):
    """
    返回国家/地区信息。
    @param refetch: 强制重新读取信息。
    """
    info = getattr(region_info, 'region_info')
    if info and not refetch:
        return info

    info = {}
    with RoseVisionDb(getattr(global_settings, 'DATABASE')['DB_SPEC']) as db:
        for code, code3, name_e, name_c, currency, weight, status in db.query_match(
                ['iso_code', 'iso_code3', 'name_e', 'name_c', 'currency', 'weight', 'status'], 'region_info',
                {}).fetch_row(maxrows=0):
            weight = int(weight)
            status = int(status)
            name_e = name_e.decode('utf-8') if name_e else None
            name_c = name_c.decode('utf-8') if name_c else None
            info[code] = {'iso_code3': code3, 'name_e': name_e, 'name_c': name_c, 'currency': currency,
                          'weight': weight, 'status': status}

    setattr(region_info, 'region_info', info)
    return info


@static_var('currency_info', None)
def currency_info(refetch=False):
    """
    返回货币信息。
    @param refetch: 强制重新读取信息。
    """
    info = getattr(currency_info, 'currency_info')
    if info and not refetch:
        return info

    info = {}
    with RoseVisionDb(getattr(global_settings, 'DATABASE')['DB_SPEC']) as db:
        for currency, symbol, name, rate, update_time in db.query_match(
                ['currency', 'symbol', 'name', 'rate', 'update_time'], 'currency_info', {}).fetch_row(maxrows=0):
            currency = currency.decode('utf-8')
            symbol = symbol.decode('utf-8') if symbol else None
            name = name.decode('utf-8') if name else None
            rate = float(rate) if rate else None
            update_time = datetime.datetime.strptime(update_time,'%Y-%m-%d %H:%M:%S') if update_time else None
            info[currency] = {'symbol': symbol, 'name': name, 'rate': rate, 'update_time': update_time}

    setattr(currency_info, 'currency_info', info)
    return info


@static_var('brand_info', None)
def brand_info(refetch=False):
    """
    返回品牌信息。
    @param refetch: 强制重新读取信息。
    """
    info = getattr(brand_info, 'brand_info')
    if info and not refetch:
        return info

    info = {}
    with RoseVisionDb(getattr(global_settings, 'DATABASE')['DB_SPEC']) as db:
        for brand_id, name_e, name_c, name_s in db.query_match(
                ['brand_id', 'brandname_e', 'brandname_c', 'brandname_s'],
                'brand_info', {}).fetch_row(maxrows=0):
            brand_id = int(brand_id)
            name_e = name_e.decode('utf-8') if name_e else None
            name_c = name_c.decode('utf-8') if name_c else None
            name_s = name_s.decode('utf-8') if name_s else None
            info[brand_id] = {'brandname_e': name_e, 'brandname_c': name_c, 'brandname_s': name_s}

    setattr(brand_info, 'brand_info', info)
    return info


@static_var('spider_info', None)
def spider_info(refetch=False):
    """
    搜索spider路径，将其中的spider按照brand_id注册起来。
    @param refetch: 强制重新读取信息。
    """
    info = getattr(spider_info, 'spider_info')
    if info and not refetch:
        return info

    info = {}
    for importer, modname, ispkg in pkgutil.iter_modules(scrapper.spiders.__path__):
        if ispkg:
            continue

        try:
            ret = importer.find_module(modname)
            submodule_list = imp.load_module(modname, ret.file, ret.filename, ret.etc)

            sc_list = filter(
                lambda val: isinstance(val[1], type) and issubclass(val[1], MFashionSpider) and val[
                    1] != MFashionSpider,
                inspect.getmembers(submodule_list))

            for sc_name, sc in sc_list:
                spider_data = getattr(sc, 'spider_data', None)
                if not spider_data:
                    continue
                if 'brand_id' in spider_data:
                    cmdname = '_'.join(modname.split('_')[:-1])
                    info[spider_data['brand_id']] = {'modname': modname, 'cmdname': cmdname}

        except (IndexError, ImportError):
            continue
        finally:
            ret.file.close()

    setattr(spider_info, 'spider_info', info)
    return info
