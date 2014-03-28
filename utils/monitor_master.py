# coding=utf-8
import os
import sys
import scripts
from utils.utils_core import get_logger
from core import RoseVisionDb
import global_settings as gs
import datetime
import psutil
import json


class MonitorMaster(object):
    @classmethod
    def run(cls, logger=None, **kwargs):
        logger = logger if 'logger' in kwargs else get_logger(logger_name='monitor')
        logger.info('Monitor STARTED!!!')

        #monitor process quantity, recrawl process quantity,limit interval for recrawl spider
        monitor_no = kwargs['monitor_no'] if 'monitor_no' in kwargs else 10
        recrawl_no = kwargs['recrawl_no'] if 'recrawl_no' in kwargs else 8
        interval = kwargs['interval'] if 'interval' in kwargs else 7
        limit_time = datetime.datetime.now() - datetime.timedelta(interval)
        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            pid_list = psutil.pids()

            rs = db.query_match(['idmonitor', 'parameter', 'monitor_status', 'monitor_pid', 'recrawl_pid'],
                                'monitor_status', {'enabled': 1}).fetch_row(maxrows=0)
            #update monitor_status

            for idmonitor, parameter, monitor_status, monitor_pid, recrawl_pid in rs:
                #更新monitor_pid,recrawl_pid
                if monitor_pid and int(monitor_pid) not in pid_list:
                    db.update({'monitor_pid': None}, 'monitor_status', str.format('idmonitor="{0}"', idmonitor))
                if recrawl_pid and int(recrawl_pid) not in pid_list:
                    db.update({'recrawl_pid': None}, 'monitor_status', str.format('idmonitor="{0}"', idmonitor))

            #更新
            rs_new = db.query_match(
                ['idmonitor', 'parameter', 'monitor_status', 'monitor_pid', 'recrawl_pid', 'timestamp'],
                'monitor_status', {'enabled': 1}, tail_str='ORDER BY priority desc, timestamp').fetch_row(maxrows=0)

            #空闲product列表，排序后，最早更新的等待monitor
            idle_monitor_list = []
            idle_recrawl_list = []
            monitor_list = []
            recrawl_list = []

            for idmonitor, parameter, monitor_status, monitor_pid, recrawl_pid, timestamp in rs_new:
                #生成monitor_pid、recrawl_pid列表，用于监控和重爬，保证数量
                if monitor_pid is not None:
                    monitor_list.append(int(monitor_pid))
                if recrawl_pid is not None:
                    recrawl_list.append(int(recrawl_pid))

                if monitor_status == '0' and monitor_pid is None and recrawl_pid is None:
                    idle_monitor_list.append((idmonitor, parameter, timestamp))
                if monitor_status == '1' and monitor_pid is None and recrawl_pid is None:
                    idle_recrawl_list.append(( idmonitor, parameter, timestamp))

                    # #爬虫最后更新时间早于最大限制时间，重爬。
                    # update_time = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                    # if update_time <= limit_time:
                    #     db.update({'monitor_status': 1}, 'monitor_status',
                    #               str.format('idmonitor={0}', idmonitor))

            # idle_monitor_list = sorted(idle_monitor_list, key=lambda m: m[2])
            # idle_recrawl_list = sorted(idle_recrawl_list, key=lambda m: m[2])

            #start monitor and set monitor_status if find update
            if len(monitor_list) < monitor_no:
                if len(idle_monitor_list) > monitor_no - len(monitor_list):
                    ready_monitor = idle_monitor_list[:(monitor_no - len(monitor_list))]
                else:
                    ready_monitor = idle_monitor_list

                for idmonitor, parameter, timestamp in ready_monitor:
                    args = json.loads(parameter)
                    #monitor --brand 10009 --region fr --idmonitor 1931 -v

                    logger.info('Monitor started--> idmonitor:%s, brand_id:%s, region:%s' % (
                    idmonitor, args['brand_id'], args['region']))

                    spawn_process(
                        os.path.join(scripts.__path__[0], 'run_crawler.py'),
                        'monitor --brand %s --region %s --idmonitor %s' % (args['brand_id'], args['region'], idmonitor))

            #start recrawl and reset monitor_status after recrawl ended
            if len(recrawl_list) < recrawl_no:
                if len(idle_recrawl_list) > recrawl_no - len(recrawl_list):
                    ready_recrawl = idle_recrawl_list[:(recrawl_no - len(recrawl_list))]
                else:
                    ready_recrawl = idle_recrawl_list

                for idmonitor, parameter, timestamp in ready_recrawl:
                    args = json.loads(parameter)
                    args['idmonitor'] = idmonitor
                    para = '|'.join([str(idmonitor), str(args['brand_id']), args['region']])

                    logger.info('Recrawl started--> idmonitor:%s, brand_id:%s, region:%s' % (
                    idmonitor, args['brand_id'], args['region']))

                    spawn_process(os.path.join(scripts.__path__[0], 'recrawler.py'), para)

        # logger.info('Monitor ENDED!!!')


def spawn_process(pyfile, args):
    #生成独立进程
    if sys.platform[:3] == 'win':
        pypath = sys.executable
        os.spawnv(os.P_NOWAIT, pypath, ('python', pyfile, args))
    else:
        pid = os.fork()
        if pid != 0:
            # print('Process %d spawned' % pid)
            pass
        else:
            os.execlp('python', 'python', pyfile, args)


if __name__ == '__main__':
    t = MonitorMaster()
    t.run()