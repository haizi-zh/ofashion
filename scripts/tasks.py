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
import memcache
from utils.db import RoseVisionDb
import urllib2

app = Celery()
app.config_from_object('scripts.celeryconfig')

# mysql_con = {"host": "173.255.255.30", "port": 3306, "schema": "celery", "username": "root", "password": "rose123"}

# command:
# export PYTHONPATH=/home/rose/MStore;
# celery -A tasks worker -l info -Q main -n main
# celery -A tasks worker -l info -Q download -n download
# celery -A tasks worker -l info -Q default -n default
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
            cycle_task = monitor_crawl.apply_async(kwargs=parameter)
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
    mc = memcache.Client(['127.0.0.1:11211'], debug=0)
    mc.set(str(str(kwargs['brand_id']) + kwargs['region']), "")

    #-----------monitor--------------
    monitor = subprocess.Popen(
        "python %s monitor --brand %s --region %s" % (
            run_crawler, kwargs['brand_id'], kwargs['region']),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, shell=True)
    monitor.communicate()

    #-----------重爬----------------
    #共享内存用于进程间通讯
    get_status = mc.get(str(str(kwargs['brand_id']) + kwargs['region']))

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
    uri = get_images_store(brand_id)
    assert uri.startswith('up://')
    info, dirpath = uri[5:].split('/', 1)
    UP_USERNAME, UP_PASSWORD, UP_BUCKETNAME = re.split('[:@]', info)
    up = upyun.UpYun(UP_BUCKETNAME, UP_USERNAME, UP_PASSWORD, timeout=30,
                     endpoint=upyun.ED_AUTO)
    full_file = os.path.join(dirpath, image_path)
    for i in range(3):
        try:
            up.put(full_file, buf.getvalue(), checksum=True)
            up.getinfo(full_file)
            break
        except Exception as e:
            if i == 2:
                #todo need to write logs to rsyslog
                raise e


def update_images(checksum, url, path, width, height, fmt, size, brand_id, model, buf, image_path):
    db = RoseVisionDb()
    db.conn(getattr(glob, 'DATABASE')['DB_SPEC'])
    db.start_transaction()
    try:
        rs1 = db.query_match('checksum', 'images_store', {'checksum': checksum})
        checksum_anchor = rs1.num_rows() > 0
        rs2 = db.query_match('checksum', 'images_store', {'path': path})
        path_anchor = rs2.num_rows() > 0

        # checksum_anchor: 说明数据库中已经有记录，其checksum和新插入的图像是一致的。
        # path_anchor：说明数据库中已经有记录，其path（也就是图像的url），和新插入的图像是一致的。
        # 这里分为4种情况讨论：
        # 1. checksum_anchor==True and path_anchor==True：说明一切正常，新增加的图像在数据库中已经有记录。
        #    不用对images_store作任何操作。
        # 2. checksum_anchor==True and path_anchor==False：数据库中已经存在这幅图像，但path不符。一般来说，可能是下面这种情况
        #    引起的：url_a和url_b这两个图像链接，指向了同样一张图像。假定数据库中已有图像记录的链接为url_a。由于url_a和url_b都存在，
        #    切对应于同一张图像，所以通常我们可以忽略url_b，不用对images_store作任何操作。
        # 3. checksum_anchor==False and path_anchor=True：二者不一致，说明该path对应图像发生了变化（比如，原网站对图像做了一
        #    些改动等，但并未更改url链接等）。此时，需要更新数据库的记录。
        # 4. checksum_anchor==False and path_anchor==False：说明这是一条全新的图像，直接入库。
        if not checksum_anchor and path_anchor:
            # 说明原来的checksum有误，整体更正
            upyun_upload(brand_id, buf, image_path)
            db.update({'checksum': checksum}, 'images_store', str.format('path="{0}"', path))
        elif not checksum_anchor and not path_anchor:
            # 在images_store里面新增一个item
            upyun_upload(brand_id, buf, image_path)
            db.insert({'checksum': checksum, 'url': url, 'path': path,
                       'width': width, 'height': height, 'format': fmt,
                       'size': size}, 'images_store', ignore=True)

        db.insert({'checksum': checksum, 'brand_id': brand_id, 'model': model,
                   'fingerprint': gen_fingerprint(brand_id, model)},
                  'products_image', ignore=True)
        db.commit()
    except:
        db.rollback()
        raise


@app.task()
def image_download(**item):

    ua = item['metadata'][
        'ua'] if 'ua' in item else 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.69 Safari/537.36'

    i_headers = {"User-Agent": ua,
                 "Accept": "text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,image/jpeg,image/gif;q=0.2,*/*;q=0.1",
    }
    for url in list(set(item['image_urls'])):
        t = time.time()
        #获取图片，重试三次
        for i in xrange(3):
            try:
                request = urllib2.Request(url, headers=i_headers)
                response = urllib2.urlopen(request)
                break
            except Exception as e:
                if i == 2:
                    #todo need to write logs to rsyslog
                    raise e
        # 确定图像类型
        tt1 = time.time()
        print 'download time:%s'%(tt1 - t)

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

        tt2 = time.time()
        print 'hash time:%s'%(tt2 - tt1)

        body = response.read()

        buf = StringIO(body)
        orig_image = Image.open(buf)
        width, height = orig_image.size
        fmt = orig_image.format

        tt3 = time.time()
        print 'image buf time:%s'%(tt3 - tt2)

        checksum = hashlib.md5(body).hexdigest()

        tt4 = time.time()
        print 'checksum time:%s'%(tt4 - tt3)

        metadata = item['metadata']
        brand_id = metadata['brand_id']
        model = metadata['model']
        size = len(body)
        region = metadata['region']

        # print('upload image:%s,%s,%s,%s,%s\n %s' % (model, region, url, image_path,checksum,item))
        # print 'pid:%s' % os.getpid()
        path = '/'.join([get_images_store(brand_id).split('/')[-1], image_path])
        #图片上传，入库顺序执行。
        t1 = time.time()
        print 'get image elapse:%s'%(t1 - tt4)
        update_images(checksum, url, path, width, height, fmt, size, brand_id, model, buf, image_path)
        t2 = time.time()
        print 'upload image elapse:%s'%(t2 - t1)
        # print('upload image:%s,%s,%s,%s\n' % (model,region, url, image_path))
