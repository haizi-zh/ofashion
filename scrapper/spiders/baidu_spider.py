from scrapy import log
from scrapy.contrib.spiders import CrawlSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.settings import Settings
from scrapper.items import BaiduItem

__author__ = 'Zephyre'

def creat_spider():
    return BaiduSpider()


class BaiduSpider(CrawlSpider):
    name = 'baidu'
    allowed_domains = ['baidu.com']
    start_urls = ['http://www.baidu.com']
    # rules = [Rule(SgmlLinkExtractor(allow=['/tor/\d+']), 'parse_torrent')]

    def parse(self, response):
        log.msg(str.format('URL={0}', response.url), log.INFO)
        s = Settings()
        log.msg(str.format('SETTINGS={0}', s.get('BOT_TEST')), log.INFO)
        x = HtmlXPathSelector(response)
        items = []
        ret = x.select("//div[@id='m']/p[@id='nv']/a[@href]")
        for node in ret:
            item = BaiduItem()
            item['description'] = node.extract()
            item['link'] = response.url
            items.append(item)
        return items

    def parse_torrent(self, response):
        x = HtmlXPathSelector(response)

        torrent = BaiduItem()
        torrent['url'] = response.url
        torrent['name'] = x.select("//h1/text()").extract()
        torrent['description'] = x.select("//div[@id='description']").extract()
        torrent['size'] = x.select("//div[@id='info-left']/p[2]/text()[2]").extract()
        return torrent
