# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class DmozItem(Item):
    title = Field()
    link = Field()
    desc = Field()


class TorrentItem(Item):
    url = Field()
    name = Field()
    description = Field()
    size = Field()


class BaiduItem(Item):
    link = Field()
    description = Field()


class ProductItem(Item):
    url = Field()
    metadata = Field()
    model = Field()
    image_urls = Field()
    images = Field()
