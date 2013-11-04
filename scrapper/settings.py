# Scrapy settings for scrapy_test project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
import global_settings as glob
import sys
import common as cm


BOT_NAME = 'scrapper'

SPIDER_MODULES = ['scrapper.spiders']
NEWSPIDER_MODULE = 'scrapper.spiders'
ITEM_PIPELINES = [
    'scrapper.pipelines.ProductImagePipeline',
    # 'scrapy.contrib.pipeline.images.ImagesPipeline',
    'scrapper.pipelines.ProductPipeline'
]

SPIDER_NAME = sys.argv[2]

EDITOR_SPEC = glob.EDITOR_SPEC
SPIDER_SPEC = glob.SPIDER_SPEC
RELEASE_SPEC = glob.RELEASE_SPEC
IMAGES_STORE = cm.get_spider_module(SPIDER_NAME).get_images_store()

LOG_LEVEL = 'INFO'
LOG_FILE = cm.get_spider_module(SPIDER_NAME).get_log_path()

# JOBDIR = cm.get_spider_module(SPIDER).get_job_path() + '-1'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'scrapper (+http://www.yourdomain.com)'
