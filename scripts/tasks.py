# coding=utf-8
from cStringIO import StringIO
import hashlib
import os
import re
import sys
import time
import Image
from upyun import upyun
from scripts.run_crawler import get_images_store
from utils.utils_core import gen_fingerprint
import global_settings as glob

sys.path.append('/home/rose/MStore')
import global_settings as gs

sys.path.append('/home/rose/MStore')
from celery import Celery
import scripts
import subprocess
import json
import mmap
from utils.db import RoseVisionDb
import urllib2

app = Celery()
app.config_from_object('scripts.celeryconfig')

# mysql_con = {"host": "173.255.255.30", "port": 3306, "schema": "celery", "username": "root", "password": "rose123"}

# command:
# export PYTHONPATH=/home/rose/MStore;
# celery -A tasks worker -l info -Q queues -n xxx
# celery flower --address=0.0.0.0 --port=5555 --broker=amqp://rose:rosecelery@localhost:5672/celery
# celery beat
#
# @app.task
# def initialize():
#     """初始化队列"""
#     with RoseVisionDb(getattr(gs, 'DATABASE')['DB_CELERY']) as db:
#         initial_tasks = db.query_match(['idmonitor', 'task_id', 'parameter'], 'crawl_status',
#                                        {'enabled': '1'}).fetch_row(maxrows=0)
#         for idmonitor, task_id, parameter in initial_tasks:
#             parameter = json.loads(parameter)
#             new_task = monitor_crawl.apply_async(kwargs=parameter)
#
#             db.update({'task_id': new_task.id, }, 'crawl_status', str.format('idmonitor={0}', idmonitor))
#
#
# @app.task
# def finalize():
#     """清空队列,需多次调用，直至队列为空"""
#     with RoseVisionDb(getattr(gs, 'DATABASE')['DB_CELERY']) as db:
#         db.update({'task_id': None, }, 'crawl_status', True)
#         db.update({'access_done': 1, }, 'taskmeta', True)


@app.task()
def main_cycle():
    """主循环，定时调用，将不同爬虫送入指定队列"""
    with RoseVisionDb(getattr(gs, 'DATABASE')['DB_CELERY']) as db:
        #add new tasks
        initial_tasks = db.query_match(['idmonitor', 'task_id', 'parameter'], 'crawl_status',
                                       {'enabled': '1', 'task_id': None}).fetch_row(maxrows=0)
        for idmonitor, task_id, parameter in initial_tasks:
            parameter = json.loads(parameter)
            parameter['idmonitor'] = idmonitor
            #添加新任务
            new_task = new_crawl.apply_async(kwargs=parameter)
            db.update({'task_id': new_task.id, }, 'crawl_status', str.format('idmonitor={0}', idmonitor))

        rs = db.query(
            str.format('SELECT taskmeta.task_id, taskmeta.status, crawl_status.idmonitor, '
                       'crawl_status.parameter FROM taskmeta JOIN crawl_status ON taskmeta.task_id = crawl_status.task_id '
            )).fetch_row(maxrows=0)
        for task_id, status, idmonitor, parameter in rs:
            parameter = json.loads(parameter)
            parameter['idmonitor'] = idmonitor
            #生成新任务
            cycle_task = monitor_crawl.apply_async(kwargs=parameter, routing_key='crawl')
            #访问完结束的任务后，将标志位置1
            #db.update({'access_done': 1, }, 'taskmeta', str.format('task_id="{0}"', task_id))
            #更新task_id
            db.update({'task_id': cycle_task.id}, 'crawl_status', str.format('idmonitor={0}', idmonitor))


@app.task()
def new_crawl(**kwargs):
    """新爬虫添加"""
    run_crawler = os.path.join(scripts.__path__[0], 'run_crawler.py')
    crawl = subprocess.Popen("python %s %s -r %s" % (run_crawler, kwargs['brand_id'], kwargs['region']),
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, shell=True)
    crawl.communicate()
    return 'crawled'


@app.task()
def monitor_crawl(**kwargs):
    """循环监视、爬取"""
    run_crawler = os.path.join(scripts.__path__[0], 'run_crawler.py')

    #共享内存用于进程间通讯
    filename = str(kwargs['brand_id']) + kwargs['region']
    fd = os.open(filename, os.O_CREAT | os.O_TRUNC | os.O_RDWR)
    assert os.write(fd, '\x00' * mmap.PAGESIZE) == mmap.PAGESIZE

    mm = mmap.mmap(fd, mmap.PAGESIZE, access=mmap.ACCESS_WRITE)
    #-----------monitor--------------
    monitor = subprocess.Popen(
        "python %s monitor --brand %s --region %s" % (
            run_crawler, kwargs['brand_id'], kwargs['region']),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, shell=True)
    monitor.communicate()

    #-----------重爬----------------
    #共享内存用于进程间通讯
    mm.seek(0)
    get_status = mm.readline()
    # print 'return status:', get_status
    # print 'recrawl' in get_status
    if 'recrawl' in get_status:
        #update
        update = subprocess.Popen(
            "python %s update --brand %s -r %s" % (run_crawler, kwargs['brand_id'], kwargs['region']),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, shell=True)
        update.communicate()
        #crawl
        crawl = subprocess.Popen("python %s %s -r %s" % (run_crawler, kwargs['brand_id'], kwargs['region']),
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, shell=True)
        crawl.communicate()
        return kwargs['brand_id'], kwargs['region'], 'recrawled'

    return kwargs['brand_id'], kwargs['region'], 'no_update'


def upyun_upload(brand_id, buf, image_path):
    try:
        uri = get_images_store(brand_id)
        assert uri.startswith('up://')
        info, dirpath = uri[5:].split('/', 1)
        UP_USERNAME, UP_PASSWORD, UP_BUCKETNAME = re.split('[:@]', info)
        up = upyun.UpYun(UP_BUCKETNAME, UP_USERNAME, UP_PASSWORD, timeout=30,
                         endpoint=upyun.ED_AUTO)
        full_file = os.path.join(dirpath, image_path)
        up.put(
            full_file, buf.getvalue(), checksum=False)
    except:
        pass


def update_images(checksum, url, path, width, height, fmt, size, brand_id, model):
    db = RoseVisionDb()
    db.conn(getattr(glob, 'DATABASE')['DB_SPEC'])
    rs = db.query_match('checksum', 'images_store', {'checksum': checksum})
    checksum_anchor = rs.num_rows() > 0
    rs = db.query_match('checksum', 'images_store', {'path': path})
    path_anchor = rs.num_rows() > 0

    if not checksum_anchor and path_anchor:
        # 说明原来的checksum有误，整体更正
        db.update({'checksum': checksum}, 'images_store', str.format('path="{0}"', path))
    elif not checksum_anchor and not path_anchor:
        # 在images_store里面新增一个item
        db.insert({'checksum': checksum, 'url': url, 'path': path,
                   'width': width, 'height': height, 'format': fmt,
                   'size': size}, 'images_store')

    db.insert({'checksum': checksum, 'brand_id': brand_id, 'model': model,
               'fingerprint': gen_fingerprint(brand_id, model)},
              'products_image', ignore=True)


@app.task()
def image_download(item):
    i_headers = {"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.1) Gecko/20090624 Firefox/3.5",
                 "Accept": "text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,image/jpeg,image/gif;q=0.2,*/*;q=0.1",
    }
    for url in item['image_urls']:
        request = urllib2.Request(url, headers=i_headers)
        response = urllib2.urlopen(request)

        # 确定图像类型
        content_type = None
        for k in response.headers:
            if k.lower() == 'content-type':
                try:
                    content_type = response.headers[k].lower()
                except (TypeError, IndexError):
                    pass
        if content_type == 'image/tiff':
            ext = 'tif'
        elif content_type == 'image/png':
            ext = 'png'
        elif content_type == 'image/gif':
            ext = 'gif'
        elif content_type == 'image/bmp':
            ext = 'bmp'
        elif content_type == 'image/jpeg':
            ext = 'jpg'
        else:
            raise
        media_guid = hashlib.sha1(url).hexdigest()
        image_path = str.format('full/{0}.{1}', media_guid, ext)

        body = response.read()

        buf = StringIO(body)
        orig_image = Image.open(buf)
        width, height = orig_image.size
        fmt = orig_image.format
        checksum = hashlib.md5(body).hexdigest()

        metadata = item['metadata']

        brand_id = metadata['brand_id']
        model = metadata['model']
        size = len(body)

        try:
            upyun_upload(brand_id, buf, image_path)
            path = '/'.join(get_images_store(brand_id).split('/')[-1], image_path)
            update_images(checksum, url, path, width, height, fmt, size, brand_id, model)
        except:
            pass






