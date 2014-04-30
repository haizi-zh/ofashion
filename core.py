# coding=utf-8
from Queue import Queue
import threading
import time
import sys

__author__ = 'Zephyre'


def func_carrier(obj, interval, estimate=False):
    th = threading.Thread(target=lambda: obj.run())
    start_ts = time.time()
    tm_ref = []
    q = Queue(maxsize=100)

    def timer_cb(last_time=False):
        if hasattr(obj, 'callback'):
            obj.callback()
        text = obj.get_msg()

        q_last = {'progress': obj.progress, 'time': time.time()}
        if q.full():
            q.get()
        q.put(q_last)

        if len(q.queue) > 1:
            head = q.queue[0]
            tail = q.queue[-1]
            speed = (tail['progress'] - head['progress']) / (tail['time'] - head['time'])
            if speed > 0:
                eta = (obj.tot - tail['progress']) / speed
                if eta > 3600:
                    hour = int(eta) / 3600
                    m = int(eta) % 3600 / 60
                    eta_str = str.format('Estimation: {0}h{1}m', hour, m)
                elif eta > 60:
                    m = int(eta) / 60
                    sec = int(eta) % 60
                    eta_str = str.format('Estimation: {0}m{1}s', m, sec)
                else:
                    eta_str = str.format('Estimation: {0}s', int(eta))
                text = str.format('{0} {1}', text, eta_str)

        sys.stdout.write('\r' + ' ' * 160 + '\r' + text)
        sys.stdout.flush()

        if not last_time:
            ts = time.time()
            next_ts = start_ts + interval * (int((ts - start_ts) / interval) + 1)
            tm = threading.Timer(next_ts - ts, timer_cb)
            tm_ref[0] = tm
            tm.start()

    tm = threading.Timer(interval, timer_cb)
    tm_ref.append(tm)

    if hasattr(obj, 'init_proc'):
        obj.init_proc()
    th.start()
    tm.start()
    th.join()
    if hasattr(obj, 'tear_down'):
        obj.tear_down()

    tm_ref[0].cancel()
    timer_cb(last_time=True)
    sys.stdout.write('\n\n')
    sys.stdout.flush()

