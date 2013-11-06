# coding=utf-8

import os
import sys
import datetime
import pydevd
from scrapy import signals, log
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor
import global_settings as glob
import common as cm

__author__ = 'Zephyre'

# 中国,美国,法国,英国,香港,日本,意大利,澳大利亚,阿联酋,新加坡,德国,加拿大,西班牙,瑞士,俄罗斯,巴西,泰国,韩国,马来西亚,荷兰
region_list = ['fr', 'us', 'cn', 'uk', 'hk', 'jp', 'it', 'au', 'ae', 'sg', 'de', 'ca', 'es', 'ch', 'ru', 'br', 'kr',
               'my', 'nl', 'tw']

debug_flag = False
debug_port = glob.DEBUG_PORT
invalid_syntax = False
ua = None

spider_name = sys.argv[1]
arguments = sys.argv[2:]
idx = 0
while True:
    if idx >= len(arguments):
        break
    term = arguments[idx]
    idx += 1
    if term == '-D':
        debug_flag = True
    elif term == '-P':
        debug_port = int(arguments[idx])
        idx += 1
    elif term == '--user-agent':
        ua = arguments[idx]
        idx += 1
    elif term == '-r':
        # 国家列表
        region_list = []
        while True:
            if idx >= len(arguments):
                break
            r = arguments[idx]
            idx += 1

            if r[0] == '-':
                # r is a command
                idx -= 1
                break
            else:
                region_list.append(r)
    else:
        print str.format('Invalid syntax: unknown command {0}', term)
        invalid_syntax = True
        break

if debug_flag:
    pydevd.settrace('localhost', port=debug_port, stdoutToServer=True, stderrToServer=True)

living_spiders = set([])


def on_spider_closed(spider, reason):
    # spider.log(str.format('spider'))
    if spider in living_spiders:
        living_spiders.remove(spider)

    if len(living_spiders) == 0:
        reactor.stop()


def get_job_path(spider_data):
    return os.path.normpath(
        os.path.join(glob.STORAGE_PATH, unicode.format(u'products/crawl/{0}', spider_data['brandname_s'])))


def get_log_path(spider_data):
    return os.path.normpath(os.path.join(glob.STORAGE_PATH, u'products/log',
                                         unicode.format(u'{0}_{1}_{2}.log', spider_data['brand_id'],
                                                        spider_data['brandname_s'],
                                                        datetime.datetime.now().strftime('%Y%m%d'))))


def get_images_store(spider_data):
    return os.path.normpath(os.path.join(glob.STORAGE_PATH, u'products/images',
                                         str.format('{0}_{1}', spider_data['brand_id'], spider_data['brandname_s'])))


def set_up_spider(region):
    crawler = Crawler(Settings())
    crawler.settings.values['BOT_NAME'] = 'mstore_bot'
    crawler.settings.values['REGION'] = region

    crawler.settings.values['ITEM_PIPELINES'] = ['scrapper.pipelines.ProductImagePipeline',
                                                 'scrapper.pipelines.ProductPipeline']
    crawler.settings.values['EXTENSIONS'] = {
        'scrapper.extensions.SpiderOpenCloseLogging': 500
    }

    crawler.settings.values['IMAGES_STORE'] = get_images_store(spider_data)
    crawler.settings.values['EDITOR_SPEC'] = glob.EDITOR_SPEC
    crawler.settings.values['SPIDER_SPEC'] = glob.SPIDER_SPEC
    crawler.settings.values['RELEASE_SPEC'] = glob.RELEASE_SPEC

    crawler.signals.connect(on_spider_closed, signal=signals.spider_closed)
    # crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()

    spider = spider_module.create_spider()
    living_spiders.add(spider)
    crawler.crawl(spider)

    crawler.start()


if not invalid_syntax:
    spider_module = cm.get_spider_module(spider_name)
    spider_data = spider_module.get_spider_data()

    for region in region_list:
        set_up_spider(region)

    log.start(loglevel='INFO', logfile=get_log_path(spider_data))
    # log.start(loglevel='DEBUG')
    log.msg(str.format('CRAWLER STARTED FOR REGIONS: {0}', ', '.join(region_list)), log.INFO)

    if len(living_spiders) > 0:
        reactor.run()   # the script will block here until the spider_closed signal was sent