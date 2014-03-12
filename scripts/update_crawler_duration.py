#coding: utf-8
from core import MySqlDb
import global_settings as gs
import os
import re
import datetime

__author__ = 'Zephyre'

def foo():
    brand = 10006
    log_path = r'G:\scrapy programs\ErrorReport\10152_gucci_20131104_1'
    file_list = []
    with MySqlDb(getattr(gs, 'DB_SPEC')) as db:
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
                st = re.findall(r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\+\d{4} \[[^\[\]]*\] INFO: Scrapy \d{1}.\d{2}.\d{1} started', lines[0])
                ed = re.findall(r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\+\d{4} \[[^\[\]]*\] INFO: Spider closed \(finished\)', lines[-1])
                if st and ed:
                    start_time = '%s-%s-%s %s:%s:%s'% (st[0])
                    end_time = '%s-%s-%s %s:%s:%s'% (ed[0])
                    duration =datetime.datetime.strptime(''.join(ed[0]),'%Y%m%d%H%M%S') - datetime.datetime.strptime(''.join(st[0]),'%Y%m%d%H%M%S')
                    break
                else:
                    start_time = 'crawling or break'
                    end_time = 'crawling or break'
                    duration = 'overtime or break'
        else:
            start_time = 'NA'
            end_time = 'NA'
            duration = 'NA'

        db.update({'start_time':start_time, 'end_time':end_time, 'duration':duration}, 'crawler_duration', str.format('brand_id={0}', brand))




