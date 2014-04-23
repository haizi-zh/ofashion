# coding=utf-8

from scrapper.spiders.update_spider import UpdateSpider
from scrapy.http import Request
import global_settings as glob
from utils.db import RoseVisionDb
from scrapy import log
import random
import json
import os

__author__ = 'Ryan'


class MonitorSpider(UpdateSpider):
    def __init__(self, parameter, db_spec, *a, **kw):
        brand = parameter['brand_id']
        region = parameter['region']

        super(MonitorSpider, self).__init__([brand], [region], db_spec, *a, **kw)
        self.name = str.format('monitor-{0}-{1}', brand, region)

        monitor_pid = os.getpid()
        # super(MonitorSpider, self).__init__(*a, **kw)
        # self.brand = brand
        # self.region = region
        # self.db = RoseVisionDb()
        # self.db.conn(db_spec)


def start_requests(self):
    # 随机取出品牌100个数据，生成request
    rs = self.db.query(str.format('SELECT * FROM products WHERE brand_id="{0}" AND region="{1}"',
                                  self.brand_list[0], self.region_list[0]))

    results = rs.fetch_row(maxrows=0, how=1)
    items = []
    for row in results:
        items += [row]
    tot_num = len(items)
    range_max = tot_num if tot_num < 100 else 100
    request_items = []
    for i in range(0, range_max):
        request_items.append(items.pop(random.randint(0, tot_num - i - 1)))

    for item in request_items:
        yield Request(url=item['url'],
                      callback=self.parse,
                      meta={'brand': int(item['brand_id']),
                            'pid': item['idproducts'],
                            'region': item['region'],
                            'model': item['model']},
                      errback=self.onerror,
                      dont_filter=True)
