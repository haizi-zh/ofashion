# coding=utf-8
import os
import sys
import json
from core import RoseVisionDb
import global_settings as gs

idm, brand, region = sys.argv[1].split('|')
brand = int(brand)
idm = int(idm)
parameter = {'idmonitor': idm, 'brand_id': brand, 'region': region}

with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
    db.update({'monitor_status': 0, 'monitor_pid': None, 'recrawl_pid': os.getpid()}, 'monitor_status',
              str.format('idmonitor={0}', parameter['idmonitor']))

os.system('python scripts/run_crawler.py update --brand %s -r %s' % (parameter['brand_id'], parameter['region']))
os.system('python scripts/run_crawler.py %s -r %s' % (parameter['brand_id'], parameter['region']))
# os.system('python scripts/mstore.py process-tags --cond brand_id=%s' % parameter['brand_id'])
# os.system('python scripts/mstore.py release --brand %s' % parameter['brand_id'])


# with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
#     db.update({'recrawl_pid': None}, 'monitor_status',
#               str.format('idmonitor={0}', parameter['idmonitor']))