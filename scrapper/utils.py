# coding=utf-8
import types
import _mysql
from scrapy import signals
from scrapy.exceptions import NotConfigured

__author__ = 'Zephyre'


class SpiderOpenCloseHandler(object):
    def __init__(self, item_count):
        self.item_count = item_count
        self.items_scraped = 0

    @classmethod
    def from_crawler(cls, crawler):
        # first check if the extension should be enabled and raise
        # NotConfigured otherwise
        db_spec = crawler.settings.get('DBSPEC')
        if not db_spec:
            raise NotConfigured

        db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                            passwd=db_spec['password'], db=db_spec['schema'])
        db.query("SET NAMES 'utf8'")
        db.query('SELECT * FROM products_tag_mapping')
        results = db.store_result().fetch_row(maxrows=0, how=1)


        # instantiate the extension object
        ext = cls(1)

        # connect the extension object to signals
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)

        # return the extension object
        return ext

    def spider_opened(self, spider):
        spider.log("opened spider %s" % spider.name)

    def spider_closed(self, spider):
        spider.log("closed spider %s" % spider.name)

    def item_scraped(self, item, spider):
        self.items_scraped += 1
        if self.items_scraped == self.item_count:
            spider.log("scraped %d items, resetting counter" % self.items_scraped)
            self.item_count = 0


def unicodify(val):
    if isinstance(val, str):
        return val.decode('utf-8')
    else:
        return val


def iterable(val):
    """
    val是否iterable。注意：val为str的话，返回False。
    :param val:
    """
    if isinstance(val, types.StringTypes):
        return False
    else:
        try:
            iter(val)
            return True
        except TypeError:
            return False


def product_tags_merge(src, dest):
    """
    合并两个tag列表：把src中的内容合并到dest中
    :param src:
    :param dest:
    """
    def to_set(val):
        """
        如果val是iterable，则转为set，否则……
        :param val:
        :return:
        """
        return set(val) if iterable(val) else set([val])

    dest = dict((k, to_set(dest[k])) for k in dest if dest[k])
    src = dict((k, to_set(src[k])) for k in src if src[k])

    for k in src:
        if k not in dest:
            dest[k] = src[k]
        else:
            dest[k] = dest[k].union(src[k])

    # 整理
    return dict((k, list(dest[k])) for k in dest)