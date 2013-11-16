# coding=utf-8
import _mysql
import datetime
import threading
import time
import sys
import common as cm

__author__ = 'Zephyre'


class MySqlDb(object):
    LOCK_READ = 0
    LOCK_WRITE = 1

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

    def insert(self, entry, table, timestamps=None, time_fmt='%Y-%m-%d %H:%M:%S'):
        """
        :param entry:
        :param table:
        :param timestamps: 用来在记录上添加时间戳。比如：timestamps=['ts1', 'ts2']，则自动在添加的记录上添加时间戳：ts1和ts2。
        """
        if timestamps:
            time_str = datetime.datetime.now().strftime(time_fmt)
            for k in timestamps:
                entry[k] = time_str

        fields = unicode.format(u'({0})', ', '.join(entry))
        values = unicode.format(u'({0})',
                                ', '.join(unicode.format(u'"{0}"', MySqlDb.sql_escape(cm.unicodify(entry[k])))
                                          if entry[k] else 'NULL' for k in entry))
        statement = unicode.format(u'INSERT INTO {0} {1} VALUES {2}', table, fields, values)
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
                           unicode.format(u'"{0}"', MySqlDb.sql_escape(cm.unicodify(entry[k]))) if entry[
                               k] else 'NULL') for k in entry)
        statement = unicode.format(u'UPDATE {0} SET {1} WHERE {2}', table, values, cond)
        self.db.query(statement.encode('utf-8'))

    def query(self, statement, use_result=False):
        self.db.query(statement.encode('utf-8'))
        return self.db.use_result() if use_result else self.db.store_result()

    def execute(self, statement):
        self.db.query(statement.encode('utf-8'))


def func_carrier(obj, interval):
    th = threading.Thread(target=lambda: obj.run())
    start_ts = time.time()
    tm_ref = []

    def timer_cb(last_time=False):
        if hasattr(obj, 'callback'):
            obj.callback()
        text = obj.get_msg()
        sys.stdout.write(' ' * 80 + '\r' + text + '\r')
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