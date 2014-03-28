#!/usr/bin/env python
# coding=utf-8
import csv
import inspect
import os
import pkgutil
import re
import sys
import hashlib
import json
import urlparse
import imp
from core import RoseVisionDb
import core
import global_settings
import scrapper.spiders

# import pydevd

# pydevd.settrace('localhost', port=7100, stdoutToServer=True, stderrToServer=True)
from scheduler import monitor
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.info import spider_info

from utils.utils_core import gen_fingerprint

SALT_PLAIN = 'roseVision88'
SALT_md5 = hashlib.md5()
SALT_md5.update(SALT_PLAIN)
SALT = SALT_md5.digest()


def encrypt(idstores):
    md5 = hashlib.md5()
    md5.update(idstores)
    d1 = md5.digest()
    d2 = ''.join(map((lambda x, y: '{0:x}'.format((ord(x) + ord(y)) % 256)), d1, SALT))
    return d2


class Sandbox(object):
    """
    随意
    """

    def __init__(self, param=None):
        self.tot = 1
        self.progress = 0
        if param and 'brand' in param:
            self.brand_list = [int(val) for val in param['brand']]
        else:
            self.brand_list = None

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def run(self):
        with RoseVisionDb(getattr(global_settings, 'DB_SPEC')) as db:
            try:
                rs = db.query(
                    'SELECT idproducts_image, brand_id, model FROM products_image where fingerprint is null').fetch_row(
                    maxrows=0)
                self.tot = len(rs)
                self.progress = 0
                now_brand = 0
                for iid, brand_id, model in rs:
                    self.progress += 1
                    fp = gen_fingerprint(brand_id, model)
                    db.update({'fingerprint': fp}, 'products_image', str.format('idproducts_image={0}', iid))
            except:
                raise


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
                yield brand_id, region, modname
        except (KeyError, AttributeError):
            continue


def update_scheduler():
    """
    根据z_online_schedule_info表的内容，将有效地爬虫写入monitor_status表

    """
    valid_brand = [10057, 10074, 10226, 10135, 10300, 10066, 13084, 10029, 10093, 10264, 10049, 10350, 10166, 10008,
                   10152, 10109, 10308, 10367, 10373, 10080, 10259, 10178, 10354, 10142, 10084, 10076, 10006, 10009,
                   10149, 10030, 10184, 10192, 10117, 10105, 10114, 10108, 10138, 10429, 10333, 10263, 10204, 10218,
                   10220, 10305, 10270, 10617, 11301, 10369, 10106, 10212, 10316]
    with RoseVisionDb(getattr(global_settings, 'DB_SPEC')) as db:
        db.start_transaction()
        try:
            # 得到已排期的爬虫
            for brand_id, region, modname in spider_generator():
                # 如果brand_id不在valid_brand列表中，则确保它也不在monitor_status中
                if brand_id not in valid_brand:
                    db.query(str.format('DELETE FROM monitor_status WHERE parameter LIKE "%{0},%"', brand_id))
                    continue
                if global_settings.region_info()[region]['status'] != 1:
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
    pass


def func1():
    brand_list = [10006, 10008, 10009, 10029, 10030, 10040, 10049, 10057, 10058, 10066, 10074, 10076, 10080, 10084,
                  10093, 10105, 10106, 10108, 10109, 10114, 10117, 10135, 10138, 10142, 10149, 10152, 10155, 10166,
                  10169, 10178, 10184, 10192, 10204, 10212, 10218, 10220, 10226, 10239, 10241, 10248, 10259, 10263,
                  10264, 10270, 10288, 10300, 10305, 10308, 10316, 10322, 10333, 10345, 10350, 10354, 10367, 10369,
                  10373, 10388, 10429, 10510, 10617, 11301, 13084]
    for brand in brand_list:
        if brand < 10259:
            continue
        cmd1 = str.format('python scripts/mstore.py process-tags --cond brand_id={0}', brand)
        cmd2 = str.format('python scripts/mstore.py release --brand {0}', brand)
        cmd = '; '.join([cmd1, cmd2])
        print cmd
        os.system(cmd)


def func2():
    # with open('missing.txt', 'r') as f:
    #     local = set(json.load(f))
    #
    # with open('missing_p.txt', 'r') as f:
    #     local_p = set(json.load(f))

    with open('fingerprint.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        remote = set([row[0].replace('"', '') for row in reader])

    with RoseVisionDb(getattr(global_settings, 'DB_SPEC')) as db:
        local = set(
            [tmp[0] for tmp in db.query('SELECT DISTINCT fingerprint FROM products_release').fetch_row(maxrows=0)])
        local_p = set(
            [tmp[0] for tmp in db.query('SELECT DISTINCT fingerprint FROM products').fetch_row(maxrows=0)])

    cnt_local = len(local)
    cnt_remote = len(remote)

    delta = remote - local
    cnt_delta = len(delta)

    delta_p = remote - local_p
    cnt_local_p = len(local_p)

    with open('missing.txt', 'w') as f:
        json.dump(list(delta), f)

    with open('missing_p.txt', 'w') as f:
        json.dump(list(delta_p), f)


if __name__ == '__main__':
    update_scheduler()
    # spider_info()
    # monitor.main()
