# coding=utf-8
from products import louis_vuitton
import sys

__author__ = 'Zephyre'


module = louis_vuitton

# ['cn', 'us', 'tw', 'kr', 'br', 'ru', 'jp', 'au', 'ca', 'it', 'es', 'de', 'uk', 'fr']

module.get_logger().info(unicode.format(u'INITIALIZING {0}...', module.__name__.upper()))

for region in sys.argv[1:]:
    module.main(region)


