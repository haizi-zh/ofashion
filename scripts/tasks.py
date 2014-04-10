# coding=utf-8
import os
from celery import Celery
import datetime
import subprocess
import scripts
import signal

app = Celery()
app.config_from_object('celeryconfig')


@app.task
def initialize():
    """初始化队列"""
    #todo initialize queues
    pass


@app.task
def main_cycle():
    """主循环，定时调用，将不同爬虫送入指定队列"""
    #todo assign routing_key to different brands, need rules
    kw = {'brand_id': 1111, 'region': 'usa', 'idmonitor': 12345}
    monitor_crawl.apply_async(kwargs=kw, exchange='vps_exchange', routing_key='vps.us')


@app.task
def monitor_crawl(**kwargs):
    """循环监视、爬取"""
    run_crawler = os.path.join(scripts.__path__[0], 'run_crawler.py')

    #-----------monitor--------------
    monitor = subprocess.Popen(
        "python %s monitor --brand %s --region %s --idmonitor %s" % (
            run_crawler, kwargs['brand_id'], kwargs['region'], kwargs['idmonitor']),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, shell=True)
    t1 = monitor.stdout.readlines()

    #-----------重爬----------------
    #todo judge monitor status
    if 'need_monitor_flag' in t1:
        #update
        update = subprocess.Popen(
            "python %s update --brand %s -r %s" % (run_crawler, kwargs['brand_id'], kwargs['region']),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, shell=True)
        #crawl
        crawl = subprocess.Popen("python %s %s -r %s" % (run_crawler, kwargs['brand_id'], kwargs['region']),
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, shell=True)
        t2 = update.stdout.readlines()
        return 'has_update', 'recrawled'

    return 'no_update', 'unrecrawled'


    # def monitor(**kwargs):
    #     run_crawler = os.path.join(scripts.__path__[0], 'run_crawler.py')
    #     monitor = subprocess.Popen(
    #         "python %s monitor --brand %s --region %s --idmonitor %s" % (
    #             run_crawler, kwargs['brand_id'], kwargs['region'], kwargs['idmonitor']), stdin=subprocess.PIPE,
    #         stdout=subprocess.PIPE, shell=True)
    #
    # def recrawl(**kwargs):
    #     run_crawler = os.path.join(scripts.__path__[0], 'run_crawler.py')
    #     #update
    #     update = subprocess.Popen(
    #         "python %s update --brand %s -r %s" % (run_crawler, kwargs['brand_id'], kwargs['region']),
    #         stdin=subprocess.PIPE,
    #         stdout=subprocess.PIPE, shell=True)
    #     #crawl
    #     crawl = subprocess.Popen("python %s %s -r %s" % (run_crawler, kwargs['brand_id'], kwargs['region']),
    #                              stdin=subprocess.PIPE,
    #                              stdout=subprocess.PIPE, shell=True)