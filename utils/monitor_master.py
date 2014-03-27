# coding=utf-8
import os
import sys
from utils.utils_core import get_logger
from core import RoseVisionDb
import global_settings as gs
import datetime
import psutil


class MonitorMaster(object):
    @classmethod
    def run(cls, logger=None, **kwargs):
        monitor_no = kwargs['monitor_no'] if 'monitor_no' in kwargs else 10
        recrawl_no = kwargs['recrawl_no'] if 'recrawl_no' in kwargs else 8
        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            pid_list = psutil.pids()

            rs = db.query_match(['idmonitor', 'parameter', 'monitor_status', 'monitor_pid', 'recrawl_pid'],
                                'monitor_status').fetch_row(maxrows=0)
            #update monitor_status
            monitor_list = []
            recrawl_list = []
            for idmonitor, parameter, monitor_status, monitor_pid, recrawl_pid in rs:

                #更新monitor_pid,recrawl_pid
                if monitor_pid and monitor_pid not in pid_list:
                    db.update({'monitor_pid': None}, 'monitor_status', str.format('idmonitor="{0}"', idmonitor))
                if recrawl_pid and recrawl_pid not in pid_list:
                    db.update({'recrawl_pid': None}, 'monitor_status', str.format('idmonitor="{0}"', idmonitor))

                #生成monitor_pid、recrawl_pid列表，用于监控和重爬，保证数量
                if monitor_pid is not None:
                    monitor_list.append(monitor_pid)
                if recrawl_pid is not None:
                    recrawl_list.append(recrawl_pid)
            #更新
            rs_new = db.query_match(
                ['idmonitor', 'parameter', 'monitor_status', 'monitor_pid', 'recrawl_pid', 'timestamp'],
                'monitor_status').fetch_row(maxrows=0)

            #空闲product列表，排序后，最早更新的等待monitor
            idle_monitor_list = []
            idle_recrawl_list = []
            for idmonitor, parameter, monitor_status, monitor_pid, recrawl_pid, timestamp in rs_new:
                if monitor_status == '0' and monitor_pid is None and recrawl_pid is None:
                    idle_monitor_list.append((idmonitor, parameter, timestamp))
                if monitor_status == '1' and monitor_pid is None and recrawl_pid is None:
                    idle_recrawl_list.append(( idmonitor, parameter, timestamp))

            idle_monitor_list = sorted(idle_monitor_list, key=lambda m: m[2])
            idle_recrawl_list = sorted(idle_recrawl_list, key=lambda m: m[2])
            # print idle_monitor_list
            # print idle_recrawl_list

            #start monitor and set monitor_status if find update
            if len(monitor_list) < monitor_no:
                if len(idle_monitor_list) > monitor_no - len(monitor_list):
                    ready_monitor = idle_monitor_list[:(monitor_no - len(monitor_list))]
                else:
                    ready_monitor = idle_monitor_list

                for idmonitor, parameter, timestamp in ready_monitor:
                    spawn_process('monitor.py', parameter)

            #start recrawl and reset monitor_status after recrawl ended
            if len(recrawl_list) < recrawl_no:
                if len(idle_recrawl_list) > recrawl_no - len(recrawl_list):
                    ready_recrawl = idle_recrawl_list[:(recrawl_no - len(recrawl_list))]
                else:
                    ready_recrawl = idle_recrawl_list

                for idmonitor, parameter, timestamp in ready_recrawl:
                    spawn_process('recrawl.py', parameter)


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