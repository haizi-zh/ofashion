from scrapper.spiders import burberry_spider, baidu_spider
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import log, signals

__author__ = 'Zephyre'

module = burberry_spider
# module = baidu_spider

spider = module.creat_spider()
crawler = Crawler(Settings())

# crawler.settings.global_defaults.EXTENSIONS = {
#     'scrapper.utils.ExtensionTest': 500
# }

crawler.settings.global_defaults.ITEM_PIPELINES = [
    'scrapper.pipelines.ProductImagePipeline',
    'scrapper.pipelines.ProductPipeline'
    # 'scrapper.pipelines.BaiduPipeline'
]

dbspec = {'host': '127.0.0.1', 'username': 'root', 'password': '123456',
          'port': 3306, 'schema': 'spider_stores'}
crawler.settings.global_defaults.IMAGES_STORE = {'path': module.get_image_path(),
                                                 'db': dbspec}
crawler.settings.global_defaults.DBSPEC = dbspec
crawler.settings.global_defaults.JOBDIR = module.get_job_path()

crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
crawler.configure()
crawler.crawl(spider)
crawler.start()
log.start(loglevel='INFO', logfile=module.get_log_path())
log.msg('CRAWLER STARTED', log.INFO)
reactor.run() # the script will block here until the spider_closed signal was sent