#coding: utf-8
from core import RoseVisionDb
import global_settings as gs
import os
import re
import datetime

__author__ = 'Zephyre'

def foo():
    brand = 10006
    log_path = r'G:\scrapy programs\ErrorReport\10152_gucci_20131104_1'
    file_list = []
    with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
        # db.update({'start_time':'', 'end_time':'', 'duration':''}, 'crawler_duration', str.format('brand_id={0}', brand))
        files = os.listdir(log_path)
        #过滤文件名
        for file in files:
            if re.search(r'^%s'%brand, file):
                file_list.append(file)
        file_list.sort(reverse=True)

        if file_list:
            for log in file_list:
                # (product, name, log_time) = re.split(r'_', os.path.basename(os.path.splitext(file)[0]))
                f = open(log_path+os.sep+log, 'rb')
                lines = f.readlines()
                st = re.findall(r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\+\d{4} \[[^\[\]]*\] INFO: Spider started, processing the following regions:', lines[0])
                ed = re.findall(r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\+\d{4} \[[^\[\]]*\] INFO: Spider closed \(finished\)', lines[-1])
                regions = re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4} \[[^\[\]]*\] INFO: Spider started, processing the following regions: (.*)', lines[0])
                if st and ed and regions:
                    start_time = '%s-%s-%s %s:%s:%s'% (st[0])
                    end_time = '%s-%s-%s %s:%s:%s'% (ed[0])
                    regions_no = len([x.strip() for x in regions[0].split(',')])
                    duration =datetime.datetime.strptime(''.join(ed[0]),'%Y%m%d%H%M%S') - datetime.datetime.strptime(''.join(st[0]),'%Y%m%d%H%M%S')

                    break
                else:
                    start_time = end_time = duration = 'overtime or break'
                    regions_no = 0

        else:
            start_time = end_time = duration = 'NA'
            regions_no = 0

        db.update({'start_time':start_time, 'end_time':end_time, 'duration':duration, 'regions_no':regions_no}, 'crawler_duration', str.format('brand_id={0}', brand))




