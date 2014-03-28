# coding=utf-8
import inspect
import pkgutil
import imp
import scrapper.spiders
from scrapper.spiders.mfashion_spider import MFashionSpider

__author__ = 'Zephyre'

# 得到各种信息


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


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
