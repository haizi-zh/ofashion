#!/usr/bin/env python
# coding=utf-8
from Queue import Queue
from itertools import ifilter
import os
import re
import shutil
import sys
import datetime
import pydevd
from scrapy import log, signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor
import global_settings as glob
import common as cm
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.utils import iterable

__author__ = 'Zephyre'


def default_error():
    print 'Invalid syntax. Use mstore help for more information.'


def argument_parser(args):
    if len(args) < 2:
        default_error()
        return

    spider_name = args[1]

    # 解析命令行参数
    param_dict = {}
    q = Queue()
    for tmp in args[2:]:
        q.put(tmp)
    param_name = None
    param_value = None
    while not q.empty():
        tmp = q.get()
        if re.search(r'--(?=[^\-])', tmp):
            tmp = re.sub('^-+', '', tmp)
            if param_name:
                param_dict[param_name] = param_value

            param_name = tmp
            param_value = None
        elif re.search(r'-(?=[^\-])', tmp):
            tmp = re.sub('^-+', '', tmp)
            for tmp in list(tmp):
                if param_name:
                    param_dict[param_name] = param_value
                    param_value = None
                param_name = tmp
        else:
            if param_name:
                if param_value:
                    param_value.append(tmp)
                else:
                    param_value = [tmp]
    if param_name:
        param_dict[param_name] = param_value

    if 'debug' in param_dict or 'D' in param_dict:
        if 'P' in param_dict:
            port = int(param_dict['P'][0])
        else:
            port = glob.DEBUG_PORT
        pydevd.settrace('localhost', port=port, stdoutToServer=True, stderrToServer=True)

    for k in ('debug', 'D', 'P'):
        try:
            param_dict.pop(k)
        except KeyError:
            pass

    return {'spider': spider_name, 'param': param_dict}


def get_job_path(brand_id):
    return os.path.normpath(
        os.path.join(glob.STORAGE_PATH,
                     unicode.format(u'products/crawl/{0}_{1}', brand_id, glob.brand_info()[brand_id]['brandname_s'])))


def get_log_path(brand_id):
    return os.path.normpath(os.path.join(glob.STORAGE_PATH, u'products/log',
                                         unicode.format(u'{0}_{1}_{2}.log', brand_id,
                                                        glob.brand_info()[brand_id]['brandname_s'],
                                                        datetime.datetime.now().strftime('%Y%m%d'))))


def get_images_store(brand_id):
    return os.path.normpath(os.path.join(
        glob.STORAGE_PATH, u'products/images', unicode.format(u'{0}_{1}', brand_id,
                                                              glob.brand_info()[brand_id]['brandname_s'])))


def set_up_spider(spider_class, region_list, data):
    crawler = Crawler(Settings())
    crawler.settings.values['BOT_NAME'] = 'mstore_bot'

    crawler.settings.values['ITEM_PIPELINES'] = {'scrapper.pipelines.ProductImagePipeline': 800,
                                                 'scrapper.pipelines.ProductPipeline': 300} \
        if glob.WRITE_DATABASE else {}

    if 'job' in data:
        job_path = get_job_path(sc.spider_data['brand_id']) + '-1'
        if 'rst-job' in data:
            shutil.rmtree(job_path, ignore_errors=True)
        crawler.settings.values['JOBDIR'] = job_path

    crawler.settings.values['AUTOTHROTTLE_ENABLED'] = True
    crawler.settings.values['TELNETCONSOLE_HOST'] = '127.0.0.1'
    if 'telnet' in data and data['telnet']:
        start_port = int(data['telnet'][0])
    else:
        start_port = spider_class.spider_data['brand_id']
    crawler.settings.values['TELNETCONSOLE_PORT'] = [start_port, start_port + 8]

    ua = data['user-agent'] if 'user-agent' in data else 'chrome'
    if ua.lower() == 'chrome':
        crawler.settings.values[
            'USER_AGENT'] = 'User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.69 Safari/537.36'
    elif ua.lower() == 'iphone':
        crawler.settings.values[
            'USER_AGENT'] = 'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_2 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8H7 Safari/6533.18.5'
    elif ua.lower() == 'ipad':
        crawler.settings.values[
            'USER_AGENT'] = 'Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B334b Safari/531.21.10'
    else:
        crawler.settings.values['USER_AGENT'] = ua

    crawler.settings.values['COOKIES_ENABLED'] = (data['cookie'].lower() == 'true') if 'cookie' in data else True

    crawler.settings.values['IMAGES_STORE'] = get_images_store(spider_class.spider_data['brand_id'])
    crawler.settings.values['IMAGES_THUMBS'] = {'small': (480, 480), 'medium': (1200, 1200)}
    crawler.settings.values['IMAGES_MIN_HEIGHT'] = 64
    crawler.settings.values['IMAGES_MIN_WIDTH'] = 64

    # crawler.signals.connect(on_spider_closed, signal=scrapy.signals.spider_closed)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()

    if not region_list:
        region_list = spider_class.get_supported_regions()
    elif not iterable(region_list):
        region_list = [region_list]

    if 'exclude-region' in data:
        for r in data['exclude-region']:
            if r in region_list:
                region_list.pop(region_list.index(r))

    spider = spider_class(region_list)
    spider.log(str.format('Spider started, processing the following regions: {0}', ', '.join(region_list)), log.INFO)
    crawler.crawl(spider)
    crawler.start()

    return spider


def main():
    cmd = argument_parser(sys.argv)
    if cmd:
        spider_module = cm.get_spider_module(cmd['spider'])
        sc_list = list(ifilter(lambda val:
                               isinstance(val, type) and issubclass(val, MFashionSpider) and val != MFashionSpider,
                               (getattr(spider_module, tmp) for tmp in dir(spider_module))))

        try:
            region_list = cmd['param']['r']
        except KeyError:
            region_list = []

        if sc_list:
            sc = sc_list[0]

            if 'v' in cmd['param'] or glob.LOG_DEBUG:
                log.start(loglevel='DEBUG')
            else:
                log.start(loglevel='INFO', logfile=get_log_path(sc.spider_data['brand_id']))

            set_up_spider(sc, region_list, cmd['param'])
            reactor.run()   # the script will block here until the spider_closed signal was sent


main()

