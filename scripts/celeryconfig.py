# coding=utf-8
from kombu import Exchange, Queue

BROKER_URL = 'amqp://spider:rose123@localhost:5672/celery'

# CELERY_RESULT_BACKEND = 'amqp://'

#add 'celery' table to mysql as the backend
CELERY_RESULT_BACKEND = 'db+mysql://root:rose123@localhost/celery'
CELERY_RESULT_DB_TABLENAMES = {
    'task': 'celery_taskmeta',
    'group': 'celery_groupmeta',
}
# CELERY_TASK_RESULT_EXPIRES = 20

#并发默认为cpu核心数，可根据需要加大，或者使用eventlet协程
#CELERYD_CONCURRENCY = 2

CELERYD_POOL_RESTARTS = True

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'Asia/Shanghai'
CELERY_ENABLE_UTC = True

from datetime import timedelta
#定时任务
CELERYBEAT_SCHEDULE = {
    "main-cycle-per-5-minutes": {
        "task": "tasks.main_cycle",
        "schedule": timedelta(minutes=5),
        "args": None
    },
}

CELERY_DEFAULT_QUEUE = 'default'
CELERY_DEFAULT_EXCHANGE_TYPE = 'direct'
CELERY_DEFAULT_ROUTING_KEY = 'default'

default_exchange = Exchange('default', type='direct')
vps_exchange = Exchange('vps', type='topic')

CELERY_QUEUES = (
    Queue('default', default_exchange, routing_key='default'),
    Queue('us', vps_exchange, routing_key='vps.us'),  #linode USA
    Queue('uk', vps_exchange, routing_key='vps.uk'),  #linode UK
    Queue('jp', vps_exchange, routing_key='vps.jp'),  #linode JAPAN

    Queue('fr', vps_exchange, routing_key='vps.fr'),  #hostvirtual FRANCE NOT READY!
    Queue('ca', vps_exchange, routing_key='vps.ca'),  #hostvirtual CANADA NOT READY!
)


class MyRouter(object):
    def route_for_task(self, task, args=None, kwargs=None):
        if task == 'myapp.tasks.compress_video':
            return {'exchange': 'video',
                    'exchange_type': 'topic',
                    'routing_key': 'video.compress'}

        elif task.startswith('dongwm.tasks.test'):
            return {
                "exchange": "broadcast_tasks",
            }
        else:
            return None


CELERY_ROUTES = (MyRouter(), )