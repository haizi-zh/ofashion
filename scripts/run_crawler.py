#!/usr/bin/env python
# coding=utf-8
# import pydevd
# pydevd.settrace('127.0.0.1', port=33333, stdoutToServer=True, stderrToServer=True)
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
import sys

sys.path.append('/home/rose/MStore')
import global_settings as glob
import common as cm
from scrapper.spiders.mfashion_spider import MFashionSpider, MFashionBaseSpider
from scrapper.spiders.eshop_spider import EShopSpider
from utils import info
from scrapper.spiders.update_spider import UpdateSpider
from utils.utils_core import parse_args
from utils.text import iterable

__author__ = 'Zephyre'


def default_error():
    print 'Invalid syntax. Use mstore help for more information.'


def get_job_path(brand_id):
    return os.path.normpath(
        os.path.join(getattr(glob, 'STORAGE')['STORAGE_PATH'],
                     unicode.format(u'products/crawl/{0}_{1}', brand_id, info.brand_info()[brand_id]['brandname_s'])))


def get_log_path(brand_id, region_list=None):
    return os.path.normpath(os.path.join(getattr(glob, 'STORAGE')['STORAGE_PATH'], u'products/log',
                                         unicode.format(u'{0}_{1}_{2}_{3}.log', brand_id,
                                                        info.brand_info()[brand_id]['brandname_s'],
                                                        datetime.datetime.now().strftime('%Y%m%d'),
                                                        '_'.join(region_list) if region_list else 'all')))


def get_images_store(brand_id):
    return os.path.join(
        getattr(glob, 'STORAGE')['IMAGE_STORE_PATH'], u'products/images', unicode.format(u'{0}_{1}', brand_id,
                                                                                         info.brand_info()[brand_id][
                                                                                             'brandname_s']))


def set_up_spider(spider_class, data, spider_type='default'):
    """
    设置爬虫对象
    @param spider_type: 爬虫类型，是update还是monitor，还是普通爬虫
    @param spider_class:
    @param data: 爬虫的配置参数
    @return:
    """

    crawler = Crawler(Settings())
    crawler.settings.values['BOT_NAME'] = 'mstore_bot'

    if spider_type == 'update':
        crawler.settings.values['ITEM_PIPELINES'] = {'scrapper.pipelines.UpdatePipeline': 800}
        brand_list = [int(tmp) for tmp in (data['brand'] if 'brand' in data else [])]
        if 'region' in data:
            region_list = data['region']
        elif 'r' in data:
            region_list = data['r']
        else:
            region_list = None
        spider = spider_class(brand_list, region_list, getattr(glob, 'DATABASE')['DB_SPEC'])
        welcome_msg = str.format('Updating started, processing the following brands: {0}',
                                 ', '.join(str(tmp) for tmp in brand_list))
    elif spider_type == 'monitor':
        crawler.settings.values['ITEM_PIPELINES'] = {'scrapper.pipelines.MonitorPipeline': 800}
        brand = int(data['brand'][0])
        region = data['region'][0]
        parameter = {'brand_id': brand, 'region': region}
        spider = spider_class(parameter, getattr(glob, 'DATABASE')['DB_SPEC'])
        welcome_msg = str.format('STARTING MONITORING, brand={0}, region={1}', brand,
                                 region)
    else:
        crawler.settings.values['ITEM_PIPELINES'] = {'scrapper.pipelines.ProductImagePipeline': 800,
                                                     'scrapper.pipelines.ProductPipeline': 300} \
            if getattr(glob, 'DATABASE')['WRITE_DATABASE'] else {}
        if 'job' in data:
            job_path = get_job_path(spider_class.spider_data['brand_id']) + '-1'
            if 'rst-job' in data:
                shutil.rmtree(job_path, ignore_errors=True)
            crawler.settings.values['JOBDIR'] = job_path

        # Telnet支持
        # crawler.settings.values['TELNETCONSOLE_HOST'] = '127.0.0.1'
        # if 'telnet' in data and data['telnet']:
        #     start_port = int(data['telnet'][0])
        # else:
        #     start_port = spider_class.spider_data['brand_id']
        # crawler.settings.values['TELNETCONSOLE_PORT'] = [start_port, start_port + 8]

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

        region_list = filter(lambda val: info.region_info()[val]['status'], region_list)

        if 'exclude-region' in data:
            for r in data['exclude-region']:
                if r in region_list:
                    region_list.pop(region_list.index(r))

        spider = spider_class(region_list)
        welcome_msg = str.format('Spider started, processing the following regions: {0}', ', '.join(region_list))

    crawler.settings.values['AUTOTHROTTLE_ENABLED'] = False

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

    # 设置spider的proxy信息
    crawler.settings.values['DOWNLOADER_MIDDLEWARES'] = {
        'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': 1}
    if 'proxy' in data:
        try:
            crawler.settings.values['PROXY_ENABLED'] = True
        except AttributeError:
            crawler.settings.values['PROXY_ENABLED'] = False
    else:
        crawler.settings.values['PROXY_ENABLED'] = False

    # TODO deal with cookies
    # cookie_flag = getattr(glob, 'COOKIES_ENABLED', False)
    # try:
    #     cookie_flag = (data['cookie'][0].lower() == 'true')
    # except (IndexError, KeyError):
    #     pass
    # crawler.settings.values['COOKIES_ENABLED'] = cookie_flag
    #
    # try:
    #     crawler.settings.values['COOKIES_DEBUG'] = getattr(glob, 'DEBUG')['COOKIES_DEBUG']
    # except (AttributeError, KeyError):
    #     crawler.settings.values['COOKIES_DEBUG'] = False

    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()

    spider.log(welcome_msg, log.INFO)
    crawler.crawl(spider)
    crawler.start()

    return spider


def main():
    logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
    logger = logging.getLogger()

    argv = sys.argv

    if len(argv) == 2 and isinstance(argv[1], str):
        # 一长串字符串，需要拆分
        tmp = argv[1].split(' ')
        del argv[1]
        argv.extend(tmp)

    ret = parse_args(argv)
    if ret:
        cmd = ret['cmd']
        param = ret['param']

        if cmd:
            # 如果输入的不是spider名称，而是品牌编号，则进行查找
            try:
                brand_id = int(cmd)
                spider_module = cm.get_spider_module(info.spider_info()[brand_id]['cmdname'])
            except ValueError:
                spider_module = cm.get_spider_module(cmd)
            except (KeyError, IOError):
                spider_module = None

            if cmd == 'update':
                spider_class = MFashionBaseSpider
                spider_type = 'update'
            elif cmd == 'monitor':
                spider_class = UpdateSpider
                spider_type = 'monitor'
            else:
                spider_class = MFashionSpider
                spider_type = 'default'

            # spider_class = MFashionBaseSpider if cmd == 'update' else MFashionSpider
            eshop_spider_class = EShopSpider

            sc_list = list(ifilter(lambda val:
                                   isinstance(val, type) and issubclass(val,
                                                                        spider_class) and val != spider_class and val != eshop_spider_class,
                                   (getattr(spider_module, tmp) for tmp in dir(spider_module))))

            if 'r' not in param:
                param['r'] = []

            if sc_list:
                sc = sc_list[0]

                if 'v' in param:
                    log.start(loglevel='DEBUG')
                else:
                    if spider_type == 'update':
                        logfile = os.path.normpath(
                            os.path.join(getattr(glob, 'STORAGE')['STORAGE_PATH'], u'products/log',
                                         unicode.format(u'update_{0}_{1}.log',
                                                        '_'.join(param['brand']),
                                                        datetime.datetime.now().strftime(
                                                            '%Y%m%d'))))
                    elif spider_type == 'default':
                        logfile = get_log_path(sc.spider_data['brand_id'], region_list=param['r'])
                    elif spider_type == 'monitor':
                        logfile = os.path.normpath(
                            os.path.join(getattr(glob, 'STORAGE')['STORAGE_PATH'], u'products/log',
                                         unicode.format(u'monitor_{0}_{1}.log',
                                                        '_'.join(param['brand']),
                                                        datetime.datetime.now().strftime(
                                                            '%Y%m%d'))))
                    else:
                        logfile = None

                    if logfile:
                        log.start(loglevel='INFO', logfile=logfile)
                    else:
                        log.start(loglevel='INFO')

                set_up_spider(sc, param, spider_type=spider_type)
                reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
    main()