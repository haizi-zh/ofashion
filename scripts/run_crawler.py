# coding=utf-8

import os
import sys

__author__ = 'Zephyre'

# 中国,美国,法国,英国,香港,日本,意大利,澳大利亚,阿联酋,新加坡,德国,加拿大,西班牙,瑞士,俄罗斯,巴西,泰国,韩国,马来西亚,荷兰

region_list = ['cn', 'us', 'fr', 'uk', 'hk', 'jp', 'it', 'au', 'ae', 'sg', 'de', 'ca', 'es', 'ch', 'ru', 'br', 'kr',
               'my', 'nl']
# region_list = ['us', 'fr', 'uk', 'hk', 'jp', 'it', 'au', 'ae', 'sg', 'de', 'ca', 'es', 'ch', 'ru', 'br', 'kr',
#                'my', 'nl']
# region_list = ['nl']

# region_list = ['ae', 'sg', 'de']
# region_list = ['ca', 'es', 'ch', 'ru', 'br', 'kr', 'my', 'nl']

spider = sys.argv[1]
for region in region_list:
    os.system(str.format('echo "Processing region: {0}"', region))
    os.system(str.format('scrapy crawl {0} -s REGION={1}', spider, region))
    os.system('echo "Completed."')
