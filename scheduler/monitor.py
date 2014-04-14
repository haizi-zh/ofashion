# coding=utf-8
import inspect
import json
import pkgutil
import imp
from core import RoseVisionDb
import global_settings
import scrapper.spiders
from scrapper.spiders.mfashion_spider import MFashionSpider
import datetime
from utils import info

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

        try:
            brand_id = sc_class.spider_data['brand_id']
            for region in sc_class.get_supported_regions():
                if brand_id < 10000:
                    continue
                if info.region_info[region]['status'] != 1:
                    continue
                yield brand_id, region, modname
        except (KeyError, AttributeError):
            continue


def main():
    with RoseVisionDb(getattr(global_settings, 'DATABASE')['DB_SPEC']) as db:
        db.start_transaction()
        try:
            for brand_id, region, modname in spider_generator():
                if info.region_info()[region]['status'] != 1:
                    continue
                parameter = {'brand_id': brand_id, 'region': region}

                # 检查是否存在
                ret = db.query(str.format('SELECT * FROM monitor_status WHERE parameter LIKE "%{0}%{1}%"', brand_id,
                                          region)).fetch_row(maxrows=0)
                if ret:
                    continue

                db.insert({'parameter': json.dumps(parameter, ensure_ascii=True)}, 'monitor_status', replace=True)
            db.commit()
        except:
            db.rollback()
            raise


if __name__ == '__main__':
    pass
