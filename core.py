# coding=utf-8
from Queue import Queue
import _mysql
import datetime
import threading
import time
import sys
import common as cm
from utils.utils import unicodify, iterable

__author__ = 'Zephyre'


class MySqlDb(object):
    LOCK_READ = 0
    LOCK_WRITE = 1

    def __init__(self, spec=None):
        self.spec = spec
        self.db = None
        self.connected = False

    def __enter__(self):
        self.conn(self.spec)
        self.connected = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connected:
            self.db.close()
            self.connected = False

    @classmethod
    def default_spec(cls):
        """
        :return: 返回默认database specification
        """
        return {'host': '127.0.0.1', 'username': None, 'password': None, 'port': None, 'schema': None}

    @classmethod
    def build_spec(cls, spec=None, host=None, username=None, password=None, port=None, schema=None):
        if not spec:
            spec = cls.default_spec()
        else:
            spec = spec.copy()

        if host:
            spec['host'] = host
        if username:
            spec['username'] = username
        if password:
            spec['password'] = password
        if port:
            spec['port'] = port
        if schema:
            spec['schema'] = port

        return spec

    def conn(self, spec):
        self.spec = spec
        self.db = _mysql.connect(host=spec['host'], port=spec['port'], user=spec['username'], passwd=spec['password'],
                                 db=spec['schema'])
        self.db.query("SET NAMES 'utf8'")

    def lock(self, tbl_list, lock_type=LOCK_WRITE):
        statement = str.format('LOCK TABLES {0}', ', '.join(
            str.format('{0} {1}', tbl, 'READ' if lock_type == MySqlDb.LOCK_READ else 'WRITE') for tbl in tbl_list))
        self.db.query(statement)

    def unlock(self):
        self.db.query('UNLOCK TABLES')

    def start_transaction(self):
        self.db.query('START TRANSACTION')

    def commit(self):
        self.db.query('COMMIT')

    def rollback(self):
        self.db.query('ROLLBACK')

    def close(self):
        self.db.close()

    @staticmethod
    def sql_escape(param):
        return unicode(param).replace('\\', '\\\\').replace('"', '\\"') if param else None

    def insert(self, entry, table, timestamps=None, time_fmt='%Y-%m-%d %H:%M:%S', ignore=False, replace=False):
        """
        :param entry:
        :param table:
        :param timestamps: 用来在记录上添加时间戳。比如：timestamps=['ts1', 'ts2']，则自动在添加的记录上添加时间戳：ts1和ts2。
        """
        entry_list = [entry] if isinstance(entry, dict) else entry
        if not entry_list:
            return
        if timestamps:
            time_str = datetime.datetime.now().strftime(time_fmt)
            for k in timestamps:
                for entry in entry_list:
                    entry[k] = time_str

        fields = unicode.format(u'({0})', ', '.join(entry_list[0]))

        def func(entry):
            return unicode.format(u'({0})',
                                  ', '.join(unicode.format(u'"{0}"', self.sql_escape(unicodify(entry[k])))
                                            if entry[k] is not None else 'NULL' for k in entry))

        statement = unicode.format(u'{3}{4}INTO {0} {1} VALUES {2}', table, fields, ', '.join(map(func, entry_list)),
                                   'INSERT' if not replace else 'REPLACE',
                                   ' IGNORE ' if ignore else ' ')
        self.db.query(statement.encode('utf-8'))

    def update(self, entry, table, cond, timestamps=None, time_fmt='%Y-%m-%d %H:%M:%S'):
        """
        :param entry:
        :param table:
        :param cond:
        :param timestamps:
        :param time_fmt:
        """
        if timestamps:
            time_str = datetime.datetime.now().strftime(time_fmt)
            for k in timestamps:
                entry[k] = time_str

        values = ', '.join(
            unicode.format(u'{0}={1}', k,
                           unicode.format(u'"{0}"', self.sql_escape(unicodify(entry[k]))) if entry[
                               k] else 'NULL') for k in entry)
        statement = unicode.format(u'UPDATE {0} SET {1} WHERE {2}', table, values, cond)
        self.db.query(statement.encode('utf-8'))

    def query(self, statement, use_result=False):
        self.db.query(statement.encode('utf-8'))
        return self.db.use_result() if use_result else self.db.store_result()

    def query_match(self, selects, table, matches=None, extra=None, tail_str=None, use_result=False, distinct=False):
        """
        查询：相当于SELECT ... FROM ... WHERE col=val
        :param selects: 需要select的字段
        :param table: 查询的表名称
        :param matches: dict类型，查询条件
        :param extra: 其它的查询条件
        :param tail_str: 添加在查询语句末尾的字符串
        :param use_result:
        :return:
        """
        if not extra:
            extra = ['1']
        elif not iterable(extra):
            extra = [extra]

        if not iterable(selects):
            selects = [selects]

        def func(arg):
            k, v = arg
            return unicode.format(u'{0}="{1}"', k, self.sql_escape(v)) if v else unicode.format(u'{0} IS NULL', k)

        match_str = ' AND '.join(map(func, matches.items())) if matches else '1'
        extra_cond = ' AND '.join(extra)
        statement = unicode.format(u'SELECT {5} {0} FROM {1} WHERE {2} AND {3} {4}',
                                   ', '.join(selects), table, match_str, extra_cond,
                                   tail_str if tail_str else '',
                                   'DISTINCT' if distinct else '')
        self.db.query(statement.encode('utf-8'))
        return self.db.use_result() if use_result else self.db.store_result()

    def execute(self, statement):
        self.db.query(statement.encode('utf-8'))


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

