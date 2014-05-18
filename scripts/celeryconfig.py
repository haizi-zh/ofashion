# coding=utf-8
from kombu import Exchange, Queue
from celery.schedules import crontab

BROKER_URL = 'amqp://rose:rosecelery@192.168.1.111:5672/celery'
# BROKER_URL = 'amqp://rose:rosecelery@localhost:5672/celery'
# BROKER_URL = 'amqp://guest:guest@localhost:5672/celery'

# CELERY_RESULT_BACKEND = 'amqp://'

#add 'celery' table to mysql as the backend
CELERY_RESULT_BACKEND = 'db+mysql://rose:rose123@192.168.1.111/celery'
CELERY_RESULT_DB_TABLENAMES = {
    'task': 'taskmeta',
    'group': 'groupmeta',
}
# CELERY_TASK_RESULT_EXPIRES = 20

#并发默认为cpu核心数，可根据需要加大，或者使用eventlet协程
CELERYD_CONCURRENCY = 3

CELERYD_POOL_RESTARTS = True

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'Asia/Shanghai'
CELERY_ENABLE_UTC = True

from datetime import timedelta
#定时任务
CELERYBEAT_SCHEDULE = {
    # Executes main_cycle every xx minute
    "main-cycle-per-5-minutes": {
        "task": "tasks.main_cycle",
        "schedule": crontab(minute='*/5'),
        "args": None,
    },
    # # Executes every Monday morning at 7:30 A.M
    # 'add-every-monday-morning': {
    # 'task': 'tasks.initialize',
    # 'schedule': crontab(hour=0, minute=15, day_of_week=1),
    # 'args': None,
    # },
    # # Executes every Monday morning at 7:30 A.M
    # 'add-every-monday-morning1': {
    # 'task': 'tasks.initialize',
    # 'schedule': crontab(hour=0, minute=16, day_of_week=1),
    # 'args': None,
    # },
}

CELERY_DEFAULT_QUEUE = 'default'
CELERY_DEFAULT_EXCHANGE_TYPE = 'direct'
CELERY_DEFAULT_ROUTING_KEY = 'default'

default_exchange = Exchange('default', type='direct')
media_exchange = Exchange('media', type='direct')

CELERY_QUEUES = (
    Queue('default', default_exchange, routing_key='default'),
    Queue('videos', media_exchange, routing_key='media.video'),
    Queue('images', media_exchange, routing_key='media.image'),

    #main_cycle用于任务分发，置于中心节点,队列使用main做名称
    Queue('main', default_exchange, routing_key='main_cycle'),
    #image_download用于图片下载，分发给专门的下载上传节点，队列使用download做名称
    Queue('download', default_exchange, routing_key='image_download'),

)


class MyRouter(object):
    def route_for_task(self, task, args=None, kwargs=None):
        if task == 'tasks.main_cycle':
            return {
                'routing_key': 'main_cycle'}

        elif task == 'tasks.image_download':
            return {
                'routing_key': 'image_download'}

        elif task == 'tasks.new_crawl':
            return {
                'routing_key': 'default'}

        elif task == 'tasks.monitor_crawl':
            return {
                'routing_key': 'default'}
        # if task == 'app.tasks.monitor_crawl':
        #     return {
        #         'exchange': 'default',
        #         'exchange_type': 'direct',
        #         'routing_key': 'crawl'}
        # elif task == 'tasks.test':
        #     return {'exchange': 'main',
        #             'exchange_type': 'direct',
        #             'routing_key': 'main'}
        else:
            return None


CELERY_ROUTES = (MyRouter(), )