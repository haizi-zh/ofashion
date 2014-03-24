# coding=utf-8
import inspect
import pkgutil
import imp
from core import RoseVisionDb
import global_settings
import scrapper.spiders
from scrapper.spiders.mfashion_spider import MFashionSpider

__author__ = 'Zephyre'

# 爬虫监控系统。通过监控分析品牌的部分单品，实现对爬虫的调度


def spider_generator():
    """
    对系统中的爬虫/国家进行遍历
    """
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

        brand_id = sc_class.spider_data['brand_id']
        for region in sc_class.get_supported_regions():
            yield brand_id, region, modname


def get_sample(brand_id, region):
    """
    针对指定品牌指定国家，随机生成一定的样本，进行检查。
    :param brand_id:
    :param region:
    """
    with RoseVisionDb(getattr(global_settings, 'DB_SPEC')) as db:
        all_pid = [tmp[0] for tmp in
                   db.query_match('idproducts', 'products', {'brand_id': brand_id, 'region': 'region'},
                                  extra='offline=0').fetch_row(maxrows=0)]


        pass
    pass


if __name__ == '__main__':
    for brand_id, region, modname in spider_generator():
        print brand_id
