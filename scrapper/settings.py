# Scrapy settings for scrapy_test project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
import os
from scrapper.spiders import burberry_spider, fendi_spider
import global_settings

BOT_NAME = 'scrapper'

SPIDER_MODULES = ['scrapper.spiders']
NEWSPIDER_MODULE = 'scrapper.spiders'
ITEM_PIPELINES = [
    'scrapper.pipelines.ProductImagePipeline',
    # 'scrapy.contrib.pipeline.images.ImagesPipeline',
    'scrapper.pipelines.ProductPipeline'
]

DBSPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123', 'port': 3306, 'schema': 'spider_stores'}
IMAGES_STORE = os.path.normpath(os.path.join(global_settings.HOME_PATH, u'products/images'))
REGION_LIST = ['cn', 'us', 'fr', 'it']

LOG_LEVEL = 'DEBUG'
# LOG_FILE = burberry_spider.get_log_path()
# JOBDIR = burberry_spider.get_job_path() + '-1'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'scrapper (+http://www.yourdomain.com)'
