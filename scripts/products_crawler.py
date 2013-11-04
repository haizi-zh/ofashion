import sys
import global_settings as glob
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import log, signals
import common as cm

__author__ = 'Zephyre'

spider_name = sys.argv[-1]
spider_module = cm.get_spider_module(spider_name)
spider = spider_module.create_spider()

crawler = Crawler(Settings())

crawler.settings.values['ITEM_PIPELINES'] = [
    'scrapper.pipelines.ProductImagePipeline',
    'scrapper.pipelines.ProductPipeline'
]

crawler.settings.values['IMAGES_STORE'] = spider_module.get_images_store()
crawler.settings.values['REGION'] = 'us'

crawler.settings.values['EDITOR_SPEC'] = glob.EDITOR_SPEC
crawler.settings.values['SPIDER_SPEC'] = glob.SPIDER_SPEC
crawler.settings.values['RELEASE_SPEC'] = glob.RELEASE_SPEC

# crawler.settings.values[
#     'USER_AGENT'] = 'Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B334b Safari/531.21.10'

# crawler.settings.values['AUTOTHROTTLE_ENABLED'] = True
# crawler.settings.values['JOBDIR'] = module.get_job_path() + '-1'
# crawler.settings.values['TELNETCONSOLE_PORT'] = [6023, 6080]

# # Email settings
# crawler.settings.values['MAIL_FROM'] = 'buddy@mfashion.com.cn'
# crawler.settings.values['MAIL_HOST'] = 'smtp.exmail.qq.com'
# crawler.settings.values['MAIL_PORT'] = 25
# crawler.settings.values['MAIL_USER'] = 'buddy@mfashion.com.cn'
# crawler.settings.values['MAIL_PASS'] = 'rose123'
# # MAIL_TLS = False
# crawler.settings.values['MAIL_SSL'] = True

crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
crawler.configure()
crawler.crawl(spider)
crawler.start()

# log.start(loglevel='DEBUG', logfile=spider_module.get_log_path())
log.start(loglevel='DEBUG')
log.msg('CRAWLER STARTED', log.INFO)
reactor.run() # the script will block here until the spider_closed signal was sent