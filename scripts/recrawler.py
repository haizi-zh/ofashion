# coding=utf-8
import os
import sys
import json
from utils.db import RoseVisionDb
import global_settings as gs
import scripts
from utils.utils_core import get_logger

idm, brand, region = sys.argv[1].split('|')
brand = int(brand)
idm = int(idm)
parameter = {'idmonitor': idm, 'brand_id': brand, 'region': region}

with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
    db.update({'monitor_status': 0, 'monitor_pid': None, 'recrawl_pid': os.getpid()}, 'monitor_status',
              str.format('idmonitor={0}', parameter['idmonitor']))
#todo hardcode for DKNY, need to add 'is_offline' for DknySpider
if brand == 10108:
    os.system(
        'python %s %s -r %s' % (
            os.path.join(scripts.__path__[0], 'run_crawler.py'), parameter['brand_id'], parameter['region']))
else:
    os.system('python %s update --brand %s -r %s' % (
        os.path.join(scripts.__path__[0], 'run_crawler.py'), parameter['brand_id'], parameter['region']))
    os.system(
        'python %s %s -r %s' % (
            os.path.join(scripts.__path__[0], 'run_crawler.py'), parameter['brand_id'], parameter['region']))
# os.system('python %s process-tags --cond brand_id=%s' % parameter['brand_id'])
# os.system('python %s release --brand %s' % parameter['brand_id'])


with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
    db.update({'recrawl_pid': None, 'priority': None}, 'monitor_status',
              str.format('idmonitor={0}', parameter['idmonitor']))

logger = get_logger(logger_name='monitor')

logger.info('Recrawl ended--> idmonitor:%s, brand_id:%s, region:%s' % (
    parameter['idmonitor'], parameter['brand_id'], parameter['region']))
