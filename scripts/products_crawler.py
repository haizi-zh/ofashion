from scrapper.spiders import burberry_spider, baidu_spider, fendi_spider
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import log, signals

__author__ = 'Zephyre'

# module = fendi_spider
module = burberry_spider
# for region in ('cn', 'us', 'fr'):

spider = module.creat_spider()
crawler = Crawler(Settings())

crawler.settings.values['ITEM_PIPELINES'] = [
    'scrapper.pipelines.ProductImagePipeline',
    'scrapper.pipelines.ProductPipeline'
]

crawler.settings.values['DBSPEC'] = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
                                     'port': 3306, 'schema': 'spider_stores'}
crawler.settings.values['IMAGES_STORE'] = module.get_image_path()
crawler.settings.values['REGION_LIST'] = ['cn']#, 'us', 'fr', 'it']
# crawler.settings.values['AUTOTHROTTLE_ENABLED'] = True
# crawler.settings.values['JOBDIR'] = module.get_job_path() + '-1'
# crawler.settings.values['TELNETCONSOLE_PORT'] = [6023, 6080]

crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
crawler.configure()
crawler.crawl(spider)
crawler.start()

# log.start(loglevel='INFO', logfile=module.get_log_path())
log.start()
log.msg('CRAWLER STARTED', log.INFO)
reactor.run() # the script will block here until the spider_closed signal was sent