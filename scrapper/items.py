# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class ProductItem(Item):
    url = Field()
    metadata = Field()
    model = Field()
    image_urls = Field()
    images = Field()


class UpdateItem(Item):
    idproduct = Field()
    brand = Field()
    region = Field()
    metadata = Field()
    offline = Field()