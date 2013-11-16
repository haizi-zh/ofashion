#!/usr/bin/env python
# coding=utf-8
import hashlib

import os
import random
import sys
import datetime
import time
import pydevd
from scrapy import signals, log
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor
import global_settings as glob
import common as cm

__author__ = 'Zephyre'

# # 中国,美国,法国,英国,香港,日本,意大利,澳大利亚,阿联酋,新加坡,德国,加拿大,西班牙,瑞士,俄罗斯,巴西,泰国,韩国,马来西亚,荷兰
# region_list = ['fr', 'us', 'cn', 'uk', 'hk', 'jp', 'it', 'au', 'ae', 'sg', 'de', 'ca', 'es', 'ch', 'ru', 'br', 'kr',
#                'my', 'nl', 'tw']

debug_flag = False
debug_port = glob.DEBUG_PORT
invalid_syntax = False
ua = None

spider_name = sys.argv[1]
arguments = sys.argv[2:]
idx = 0
# 国家列表
region_list = []
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
    if spider in living_spiders:
        living_spiders.remove(spider)

    if len(living_spiders) == 0:
        reactor.stop()


def get_job_path(brand_id):
    return os.path.normpath(
        os.path.join(glob.STORAGE_PATH,
                     unicode.format(u'products/crawl/{0}', glob.BRAND_NAMES[brand_id]['brandname_s'])))


def get_log_path(brand_id):
    return os.path.normpath(os.path.join(glob.STORAGE_PATH, u'products/log',
                                         unicode.format(u'{0}_{1}_{2}.log', brand_id,
                                                        glob.BRAND_NAMES[brand_id]['brandname_s'],
                                                        datetime.datetime.now().strftime('%Y%m%d'))))


def get_images_store(brand_id):
    return os.path.normpath(os.path.join(glob.STORAGE_PATH, u'products/images',
                                         str.format('{0}_{1}', brand_id, glob.BRAND_NAMES[brand_id]['brandname_s'])))


def set_up_spider(region):
    crawler = Crawler(Settings())
    crawler.settings.values['BOT_NAME'] = 'mstore_bot'
    crawler.settings.values['REGION'] = region

    crawler.settings.values['ITEM_PIPELINES'] = {'scrapper.pipelines.ProductImagePipeline': 300,
                                                 'scrapper.pipelines.ProductPipeline': 800} \
        if glob.WRITE_DATABASE else {}

    crawler.settings.values['EXTENSIONS'] = {
        'scrapper.extensions.SpiderOpenCloseLogging': 500
    }

    crawler.settings.values['IMAGES_STORE'] = get_images_store(spider_module.brand_id)
    spider = spider_module.create_spider()

    # crawler.settings.values['JOBDIR'] = get_job_path(spider_module.brand_id) + str.format('-{0}-1', region)

    crawler.settings.values['EDITOR_SPEC'] = glob.EDITOR_SPEC
    crawler.settings.values['SPIDER_SPEC'] = glob.SPIDER_SPEC
    crawler.settings.values['RELEASE_SPEC'] = glob.RELEASE_SPEC

    crawler.settings.values['TELNETCONSOLE_PORT'] = [7023, 7073]
    # crawler.settings.values['RETRY_TIMES'] = 0
    # crawler.settings.values['REDIRECT_ENABLED'] = False

    crawler.signals.connect(on_spider_closed, signal=signals.spider_closed)
    # crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()

    living_spiders.add(spider)
    crawler.crawl(spider)

    crawler.start()


if not invalid_syntax:
    spider_module = cm.get_spider_module(spider_name)
    if not region_list:
        region_list = spider_module.supported_regions()

    for region in region_list:
        set_up_spider(region)

    if glob.LOG_DEBUG:
        log.start(loglevel='DEBUG')
    else:
        log.start(loglevel='INFO', logfile=get_log_path(spider_module.brand_id))

    log.msg(str.format('CRAWLER STARTED FOR REGIONS: {0}', ', '.join(region_list)), log.INFO)

    if len(living_spiders) > 0:
        reactor.run()   # the script will block here until the spider_closed signal was sent