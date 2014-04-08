# coding=utf-8
import inspect
import pkgutil
import imp
import scrapper.spiders
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.db import RoseVisionDb
import global_settings

__author__ = 'Zephyre'

# 得到各种信息


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


@static_var('region_info', None)
def region_info():
    """
    返回国家/地区信息
    """
    info = getattr(region_info, 'region_info')
    if info:
        return info

    info = {}
    with RoseVisionDb(getattr(global_settings, 'DB_SPEC')) as db:
        for code, currency, weight, status in db.query_match(['iso_code', 'currency', 'weight', 'status'],
                                                             'region_info', {}).fetch_row(maxrows=0):
            weight = int(weight)
            status = int(status)
            info[code] = {'currency': currency, 'weight': weight, 'status': status}

    setattr(region_info, 'region_info', info)
    return info


@static_var('spider_info', None)
def spider_info():
    """
    搜索spider路径，将其中的spider按照brand_id注册起来
    """
    info = getattr(spider_info, 'spider_info')
    if info:
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
