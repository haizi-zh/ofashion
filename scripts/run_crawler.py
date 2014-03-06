#!/usr/bin/env python
# coding=utf-8
from Queue import Queue
from itertools import ifilter
import logging
import os
import re
import shutil
import sys
import datetime
from scrapy import log, signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor
import global_settings as glob
import common as cm
from scrapper.spiders.mfashion_spider import MFashionSpider, MFashionBaseSpider
from scrapy.contrib.spiders import CrawlSpider
from scrapper.spiders.update_spider import UpdateSpider
import scrapper.spiders.update_spider as ups
from utils.utils import iterable, parse_args

__author__ = 'Zephyre'


def default_error():
    print 'Invalid syntax. Use mstore help for more information.'


def get_job_path(brand_id):
    return os.path.normpath(
        os.path.join(getattr(glob, 'STORAGE_PATH'),
                     unicode.format(u'products/crawl/{0}_{1}', brand_id, glob.brand_info()[brand_id]['brandname_s'])))


def get_log_path(brand_id):
    return os.path.normpath(os.path.join(getattr(glob, 'STORAGE_PATH'), u'products/log',
                                         unicode.format(u'{0}_{1}_{2}.log', brand_id,
                                                        glob.brand_info()[brand_id]['brandname_s'],
                                                        datetime.datetime.now().strftime('%Y%m%d'))))


def get_images_store(brand_id):
    return os.path.normpath(os.path.join(
        getattr(glob, 'STORAGE_PATH'), u'products/images', unicode.format(u'{0}_{1}', brand_id,
                                                                          glob.brand_info()[brand_id]['brandname_s'])))


def set_up_spider(spider_class, data, is_update=False):
    """
    设置爬虫对象
    @param is_update: 是否是一个UpdateSpider
    @param spider_class:
    @param data: 爬虫的配置参数
    @return:
    """

    crawler = Crawler(Settings())
    crawler.settings.values['BOT_NAME'] = 'mstore_bot'

    if is_update:
        crawler.settings.values['ITEM_PIPELINES'] = {'scrapper.pipelines.UpdatePipeline': 800}
        brand_list = [int(tmp) for tmp in (data['brand'] if 'brand' in data else [])]
        if 'region' in data:
            region_list = data['region']
        elif 'r' in data:
            region_list = data['r']
        else:
            region_list = None
        spider = spider_class(brand_list, region_list, getattr(glob,'DB_SPEC'))
    else:
        crawler.settings.values['ITEM_PIPELINES'] = {'scrapper.pipelines.ProductImagePipeline': 800,
                                                     'scrapper.pipelines.ProductPipeline': 300} if glob.WRITE_DATABASE else {}
        if 'job' in data:
            job_path = get_job_path(spider_class.spider_data['brand_id']) + '-1'
            if 'rst-job' in data:
                shutil.rmtree(job_path, ignore_errors=True)
            crawler.settings.values['JOBDIR'] = job_path

        # Telnet支持
        crawler.settings.values['TELNETCONSOLE_HOST'] = '127.0.0.1'
        if 'telnet' in data and data['telnet']:
            start_port = int(data['telnet'][0])
        else:
            start_port = spider_class.spider_data['brand_id']
        crawler.settings.values['TELNETCONSOLE_PORT'] = [start_port, start_port + 8]

        # 图像数据存储
        crawler.settings.values['IMAGES_STORE'] = get_images_store(spider_class.spider_data['brand_id'])
        crawler.settings.values['IMAGES_THUMBS'] = {'small': (480, 480), 'medium': (1200, 1200)}
        crawler.settings.values['IMAGES_MIN_HEIGHT'] = 64
        crawler.settings.values['IMAGES_MIN_WIDTH'] = 64

        # 获取爬虫区域
        region_list = data['r']
        if not region_list:
            region_list = spider_class.get_supported_regions()
        elif not iterable(region_list):
            region_list = [region_list]

        if 'exclude-region' in data:
            for r in data['exclude-region']:
                if r in region_list:
                    region_list.pop(region_list.index(r))

        spider = spider_class(region_list)

    crawler.settings.values['AUTOTHROTTLE_ENABLED'] = True

    # 设置spider的user agent
    ua = data['user-agent'][0] if 'user-agent' in data else 'chrome'
    if ua.lower() == 'chrome':
        crawler.settings.values[
            'USER_AGENT'] = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.69 Safari/537.36'
    elif ua.lower() == 'iphone':
        crawler.settings.values[
            'USER_AGENT'] = 'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_2 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8H7 Safari/6533.18.5'
    elif ua.lower() == 'ipad':
        crawler.settings.values[
            'USER_AGENT'] = 'Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B334b Safari/531.21.10'
    else:
        crawler.settings.values['USER_AGENT'] = ua

    cookie_flag = getattr(glob, 'COOKIES_ENABLED', False)
    try:
        cookie_flag = (data['cookie'][0].lower() == 'true')
    except (IndexError, KeyError):
        pass
    crawler.settings.values['COOKIES_ENABLED'] = cookie_flag

    crawler.settings.values['COOKIES_DEBUG'] = getattr(glob, 'COOKIES_DEBUG', False)

    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()

    if is_update:
        spider.log(str.format('Updating started, processing the following brands: {0}',
                              ', '.join(str(tmp) for tmp in brand_list)), log.INFO)
    else:
        spider.log(str.format('Spider started, processing the following regions: {0}', ', '.join(region_list)),
                   log.INFO)
    crawler.crawl(spider)
    crawler.start()

    return spider


def main():
    logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
    logger = logging.getLogger()

    ret = parse_args(sys.argv)
    if ret:
        cmd = ret['cmd']
        param = ret['param']

        if cmd:
            spider_module = cm.get_spider_module(cmd)
            spider_class = MFashionBaseSpider if cmd == 'update' else MFashionSpider
            is_update = (not spider_class == MFashionSpider)

            sc_list = list(ifilter(lambda val:
                                   isinstance(val, type) and issubclass(val, spider_class) and val != spider_class,
                                   (getattr(spider_module, tmp) for tmp in dir(spider_module))))

            if 'r' not in param:
                param['r'] = []

            if sc_list:
                sc = sc_list[0]

                if 'v' in param or getattr(glob, 'LOG_DEBUG'):
                    log.start(loglevel='DEBUG')
                else:
                    if is_update:
                        logfile = os.path.normpath(os.path.join(getattr(glob, 'STORAGE_PATH'), u'products/log',
                                                                unicode.format(u'update_{0}_{1}.log',
                                                                               '_'.join(param['brand']),
                                                                               datetime.datetime.now().strftime(
                                                                                   '%Y%m%d%H%M%S'))))
                    else:
                        logfile = get_log_path(sc.spider_data['brand_id'])
                    log.start(loglevel='INFO', logfile=logfile)

                set_up_spider(sc, param, is_update=is_update)
                reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
    main()