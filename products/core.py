# coding=utf-8
from products import louis_vuitton
from products import burberry

__author__ = 'Zephyre'


module = louis_vuitton

module.get_logger().info(unicode.format(u'INITIALIZING {0}...', module.__name__.upper()))
module.main()


