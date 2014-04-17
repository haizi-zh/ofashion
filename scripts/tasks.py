# coding=utf-8
import os
from celery import Celery
import scripts
import subprocess
import json
import mmap
from utils.db import RoseVisionDb

app = Celery()
app.config_from_object('scripts.celeryconfig')

mysql_con = {"host": "localhost", "port": 3306, "schema": "celery", "username": "root", "password": "rose123"}

# command:
# celery -A tasks worker -l info -Q queues -n xxx
# celery flower --address=127.0.0.1 --port=5555
# celery beat

@app.task
def initialize():
    """初始化队列"""
    with RoseVisionDb(mysql_con) as db:
        initial_tasks = db.query_match(['idmonitor', 'task_id', 'parameter'], 'crawl_status',
                                       {'enabled': '1'}).fetch_row(maxrows=0)
        for idmonitor, task_id, parameter in initial_tasks:
            parameter = json.loads(parameter)
            new_task = monitor_crawl.apply_async(kwargs=parameter)

            db.update({'task_id': new_task.id, }, 'crawl_status', str.format('idmonitor={0}', idmonitor))


@app.task
def finalize():
    """清空队列,需多次调用，直至队列为空"""
    with RoseVisionDb(mysql_con) as db:
        db.update({'task_id': None, }, 'crawl_status', True)
        db.update({'access_done': 1, }, 'taskmeta', True)


@app.task
def main_cycle():
    """主循环，定时调用，将不同爬虫送入指定队列"""
    with RoseVisionDb(mysql_con) as db:
        #add new tasks
        initial_tasks = db.query_match(['idmonitor', 'task_id', 'parameter'], 'crawl_status',
                                       {'enabled': '1', 'task_id': None}).fetch_row(maxrows=0)
        for idmonitor, task_id, parameter in initial_tasks:
            parameter = json.loads(parameter)
            #添加新任务
            new_task = monitor_crawl.apply_async(kwargs=parameter)
            db.update({'task_id': new_task.id, }, 'crawl_status', str.format('idmonitor={0}', idmonitor))

        rs = db.query(
            str.format('SELECT taskmeta.task_id, taskmeta.status, crawl_status.idmonitor, '
                       'crawl_status.parameter FROM taskmeta JOIN crawl_status ON taskmeta.task_id = crawl_status.task_id '
                       'WHERE taskmeta.access_done = 0')).fetch_row(maxrows=0)
        for task_id, status, idmonitor, parameter in rs:
            parameter = json.loads(parameter)
            #生成新任务
            cycle_task = monitor_crawl.apply_async(kwargs=parameter)
            #访问完结束的任务后，将标志位置1
            db.update({'access_done': 1, }, 'taskmeta', str.format('task_id="{0}"', task_id))
            #更新task_id
            db.update({'task_id': cycle_task.id}, 'crawl_status', str.format('idmonitor={0}', idmonitor))


# @app.task
# def monitor_crawl(**kwargs):
#     """循环监视、爬取"""
#     #todo for test.NEED delete!
#     print monitor_crawl.request.id
#     print kwargs['brand_id'], kwargs['region']
#     return kwargs['brand_id'], kwargs['region']


@app.task
def monitor_crawl(**kwargs):
    """循环监视、爬取"""
    #todo add mmap to subprocess
    run_crawler = os.path.join(scripts.__path__[0], 'run_crawler.py')

    #共享内存用于进程间通讯
    mm = mmap.mmap(fileno=-1, length=10, tagname=kwargs['idmonitor'], access=mmap.ACCESS_WRITE)
    #-----------monitor--------------
    monitor = subprocess.Popen(
        "python %s monitor --brand %s --region %s --idmonitor %s" % (
            run_crawler, kwargs['brand_id'], kwargs['region'], kwargs['idmonitor']),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, shell=True)
    monitor.communicate()

    #-----------重爬----------------
    #共享内存用于进程间通讯
    mm.seek(0)
    if mm.readline() == 'recrawl':
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
        return 'has_update', 'recrawled'

    return 'no_update', 'unrecrawled'

