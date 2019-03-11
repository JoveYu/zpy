# coding: utf-8
import time
import datetime
import types
import random
import logging
import threading
import traceback
from contextlib import contextmanager

log = logging.getLogger()

dbpool = None

def timeit(func):
    def _(conn, *args, **kwargs):
        starttm = time.time()
        ret = 0
        num = 0
        err = ''
        try:
            retval = func(conn, *args, **kwargs)
            if isinstance(retval, list):
                num = len(retval)
            elif isinstance(retval, dict):
                num = 1
            elif isinstance(retval, int):
                ret = retval
            return retval
        except Exception as e:
            err = e
            ret = -1
            raise
        finally:
            endtm = time.time()
            dbcf = conn.param
            log.info('ep=%s|id=%d|name=%s|user=%s|addr=%s:%d|db=%s|idle=%d|busy=%d|max=%d|trans=%d|time=%d|ret=%s|num=%d|sql=%s|err=%s',
                     conn.type, conn.conn_id%10000,
                     conn.name, dbcf.get('user',''),
                     dbcf.get('host',''), dbcf.get('port',0),
                     dbcf.get('db',''),
                     len(conn.pool.dbconn_idle),
                     len(conn.pool.dbconn_using),
                     conn.pool.max_conn, conn.trans,
                     int((endtm-starttm)*1000000),
                     str(ret), num,
                     args[0], err)
    return _


class DBPoolBase:
    def acquire(self, name):
        pass

    def release(self, name, conn):
        pass


class DBResult:
    def __init__(self, fields, data):
        self.fields = fields
        self.data = data

    def todict(self):
        ret = []
        for item in self.data:
            ret.append(dict(zip(self.fields, item)))
        return ret

    def __iter__(self):
        for row in self.data:
            yield dict(zip(self.fields, row))

    def row(self, i, isdict=True):
        if isdict:
            return dict(zip(self.fields, self.data[i]))
        return self.data[i]

    def __getitem__(self, i):
        return dict(zip(self.fields, self.data[i]))

class DBFunc:
    def __init__(self, data):
        self.value = data


class DBConnection:
    def __init__(self, param):
        self.param      = param
        self.name       = param.get('name','')
        self.conn       = None
        self.status     = 0
        self.lasttime   = time.time()
        self.pool       = None
        self.server_id  = None
        self.conn_id    = 0
        self.trans      = 0 # is start transaction
        self.role       = param.get('role', 'm') # master/slave

        self.safe_connect()

    def __str__(self):
        return '<%s %s:%d %s@%s>' % (self.type,
                self.param.get('host',''), self.param.get('port',0),
                self.param.get('user',''), self.param.get('db',0)
                )

    def is_available(self):
        return self.status == 0

    def useit(self):
        self.status = 1
        self.lasttime = time.time()

    def releaseit(self):
        self.status = 0

    def safe_connect(self):
        self.safe_close()
        self.connect()
        self.trans = 0

        log.info('server=%s|func=connect|id=%d|name=%s|user=%s|addr=%s:%d|db=%s',
                    self.type, self.conn_id%10000,
                    self.name, self.param.get('user',''),
                    self.param.get('host',''), self.param.get('port',0),
                    self.param.get('db',''))

    def safe_close(self):
        try:
            if self.conn:
                self.close()
                log.info('server=%s|func=close|id=%d|name=%s|user=%s|addr=%s:%d|db=%s',
                            self.type, self.conn_id%10000,
                            self.name, self.param.get('user',''),
                            self.param.get('host',''), self.param.get('port',0),
                            self.param.get('db',''))
        except:
            log.warn(traceback.format_exc())
        self.conn = None

    def connect(self):
        pass

    def close(self):
        self.conn.close()
        self.conn = None

    def ping(self):
        pass

    def last_insert_id(self):
        pass

    def begin(self): # start transaction
        self.trans = 1
        self.conn.begin()

    def commit(self):
        self.trans = 0
        self.conn.commit()

    def rollback(self):
        self.trans = 0
        self.conn.rollback()

    def escape(self, s):
        return s

    def cursor(self):
        return self.conn.cursor()

    @timeit
    def execute(self, sql, param=None):
        cur = self.conn.cursor()
        if param:
            if not isinstance(param, (dict, tuple)):
                param = tuple([param])
            ret = cur.execute(sql, param)
        else:
            ret = cur.execute(sql)
        cur.close()
        return ret

    @timeit
    def executemany(self, sql, param):
        cur = self.conn.cursor()
        if param:
            if not isinstance(param, (dict, tuple)):
                param = tuple([param])
            ret = cur.executemany(sql, param)
        else:
            ret = cur.executemany(sql)
        cur.close()
        return ret

    @timeit
    def query(self, sql, param=None, isdict=True, head=False):
        '''sql查询，返回查询结果'''
        cur = self.conn.cursor()
        if param:
            if not isinstance(param, (dict, tuple)):
                param = tuple([param])
            cur.execute(sql, param)
        else:
            cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        if res and isdict:
            ret = []
            xkeys = [ i[0] for i in cur.description]
            for item in res:
                ret.append(dict(zip(xkeys, item)))
        else:
            ret = res
            if head:
                xkeys = [ i[0] for i in cur.description]
                ret.insert(0, xkeys)
        return ret

    @timeit
    def get(self, sql, param=None, isdict=True):
        '''sql查询，只返回一条'''
        cur = self.conn.cursor()
        if param:
            if not isinstance(param, (dict, tuple)):
                param = tuple([param])
            cur.execute(sql, param)
        else:
            cur.execute(sql)
        res = cur.fetchone()
        cur.close()
        if res and isdict:
            xkeys = [ i[0] for i in cur.description]
            return dict(zip(xkeys, res))
        else:
            return res

    def value2sql(self, v):
        if isinstance(v, str):
            if v.startswith(('now()','md5(')):
                return v
            return "'%s'" % self.escape(v)
        elif isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
            return "'%s'" % str(v)
        elif isinstance(v, DBFunc):
            return v.value
        elif v is None:
            return 'NULL'
        else:
            return str(v)

    def exp2sql(self, key, op, value):
        item = '(`%s` %s ' % (key.replace('.','`.`'), op)
        if op == 'in':
            item += '(%s))' % ','.join([self.value2sql(x) for x in value])
        elif op == 'not in':
            item += '(%s))' % ','.join([self.value2sql(x) for x in value])
        elif op == 'between':
            item += ' %s and %s)' % (self.value2sql(value[0]), self.value2sql(value[1]))
        else:
            item += self.value2sql(value) + ')'
        return item

    def dict2sql(self, d, sp=','):
        '''字典可以是 {name:value} 形式，也可以是 {name:(operator, value)}'''
        x = []
        for k,v in d.items():
            if isinstance(v, tuple):
                x.append('%s' % self.exp2sql(k, v[0], v[1]))
            else:
                x.append('`%s`=%s' % (k.replace('.','`.`'), self.value2sql(v)))
        return sp.join(x)

    def dict2on(self, d, sp=' and '):
        x = []
        for k,v in d.items():
            x.append('`%s`=`%s`' % (k.strip(' `').replace('.','`.`'), v.strip(' `').replace('.','`.`')))
        return sp.join(x)

    def dict2insert(self, d):
        keys = []
        vals = []
        for k in sorted(d.keys()):
            vals.append('%s' % self.value2sql(d[k]))
            keys.append('`%s`' % k)
        return ','.join(keys), ','.join(vals)

    def format_table(self, table):
        '''调整table 支持加上 `` 并支持as'''
        #如果有as
        table = table.replace(',','`,`')
        index = table.find(' ')
        if ' ' in table:
            return '`%s`%s' % ( table[:index] ,table[index:])
        else:
            return '`%s`' % table

    def insert(self, table, values, other=None):
        #sql = "insert into %s set %s" % (table, self.dict2sql(values))
        keys, vals = self.dict2insert(values)
        sql = "insert into %s(%s) values (%s)" % (self.format_table(table), keys, vals)
        if other:
            sql += ' ' + other
        return self.execute(sql)

    def insert_list(self, table, values_list, other=None):
        vals_list = []
        for values in values_list:
            keys, vals = self.dict2insert(values)
            vals_list.append('(%s)' % vals)

        sql = "insert into %s(%s) values %s" % (self.format_table(table), keys, ','.join(vals_list))
        if other:
            sql += ' ' + other
        return self.execute(sql)

    def update(self, table, values, where=None, other=None):
        sql = "update %s set %s" % (self.format_table(table), self.dict2sql(values))
        if where:
            sql += " where %s" % self.dict2sql(where,' and ')
        if other:
            sql += ' ' + other
        return self.execute(sql)

    def delete(self, table, where, other=None):
        sql = "delete from %s" % self.format_table(table)
        if where:
            sql += " where %s" % self.dict2sql(where, ' and ')
        if other:
            sql += ' ' + other
        return self.execute(sql)

    def select(self, table, where=None, fields='*', other=None, isdict=True):
        sql = self.select_sql(table, where, fields, other)
        return self.query(sql, None, isdict=isdict)

    def select_one(self, table, where=None, fields='*', other='limit 1', isdict=True):
        if 'limit' not in other:
            other += ' limit 1'

        sql = self.select_sql(table, where, fields, other)
        return self.get(sql, None, isdict=isdict)

    def select_join(self, table1, table2, join_type='inner', on=None, where=None, fields='*', other=None, isdict=True):
        sql = self.select_join_sql(table1, table2, join_type, on, where, fields, other)
        return self.query(sql, None, isdict=isdict)

    def select_join_one(self, table1, table2, join_type='inner', on=None, where=None, fields='*', other='limit 1', isdict=True):
        if 'limit' not in other:
            other += ' limit 1'

        sql = self.select_join_sql(table1, table2, join_type, on, where, fields, other)
        return self.get(sql, None, isdict=isdict)

    def select_sql(self, table, where=None, fields='*', other=None):
        if isinstance(fields, (list, tuple)):
            fields = ','.join(fields)
        sql = "select %s from %s" % (fields, self.format_table(table))
        if where:
            sql += " where %s" % self.dict2sql(where, ' and ')
        if other:
            sql += ' ' + other
        return sql

    def select_join_sql(self, table1, table2, join_type='inner', on=None, where=None, fields='*', other=None):
        if isinstance(fields, (list, tuple)):
            fields = ','.join(fields)
        sql = "select %s from %s %s join %s" % (fields, self.format_table(table1), join_type, self.format_table(table2))
        if on:
            sql += " on %s" % self.dict2on(on, ' and ')
        if where:
            sql += " where %s" % self.dict2sql(where, ' and ')
        if other:
            sql += ' ' + other
        return sql


def with_mysql_reconnect(func):

    def _(self, *args, **argitems):
        if self.type == 'mysql':
            import MySQLdb as m
        elif self.type == 'pymysql':
            import pymysql as m
        trycount = 3
        while True:
            try:
                x = func(self, *args, **argitems)
            except m.OperationalError as e:
                log.warn(traceback.format_exc())
                if e[0] >= 2000 and self.trans == 0: # 客户端错误
                    self.safe_connect()
                    trycount -= 1
                    if trycount > 0:
                        continue
                raise
            except (m.InterfaceError, m.InternalError):
                log.warn(traceback.format_exc())
                if self.trans == 0:
                    self.safe_connect()
                raise
            else:
                return x
    return _

class MySQLConnection(DBConnection):
    type = "mysql"

    def connect(self):
        import MySQLdb
        self.conn = MySQLdb.connect(host = self.param['host'],
                                    port = self.param['port'],
                                    user = self.param['user'],
                                    passwd = self.param['passwd'],
                                    db = self.param['db'],
                                    charset = self.param['charset'],
                                    connect_timeout = self.param.get('timeout', 10),
                                    )

        self.conn.autocommit(1)

        cur = self.conn.cursor()
        cur.execute("show variables like 'server_id'")
        row = cur.fetchone()
        self.server_id = int(row[1])
        cur.close()

        cur = self.conn.cursor()
        cur.execute("select connection_id()")
        row = cur.fetchone()
        self.conn_id = row[0]
        cur.close()

    @with_mysql_reconnect
    def ping(self):
        if self.is_available():
            cur = self.conn.cursor()
            cur.execute("show tables")
            cur.close()
            self.conn.ping()

    def escape(self, s):
        return self.conn.escape_string(s)

    def last_insert_id(self):
        ret = self.query('select last_insert_id()', isdict=False)
        return ret[0][0]

    @with_mysql_reconnect
    def execute(self, sql, param=None):
        return DBConnection.execute(self, sql, param)

    @with_mysql_reconnect
    def executemany(self, sql, param):
        return DBConnection.executemany(self, sql, param)

    @with_mysql_reconnect
    def query(self, sql, param=None, isdict=True, head=False):
        return DBConnection.query(self, sql, param, isdict, head)

    @with_mysql_reconnect
    def get(self, sql, param=None, isdict=True):
        return DBConnection.get(self, sql, param, isdict)



class PyMySQLConnection (MySQLConnection):
    type = "pymysql"

    def connect(self):
        engine = self.param['engine']
        if engine == 'pymysql':
            import pymysql
            self.conn = pymysql.connect(host = self.param['host'],
                                        port = self.param['port'],
                                        user = self.param['user'],
                                        passwd = self.param['passwd'],
                                        db = self.param['db'],
                                        charset = self.param['charset'],
                                        connect_timeout = self.param.get('timeout', 10),
                                        )
            self.conn.autocommit(1)

            cur = self.conn.cursor()
            cur.execute("show variables like 'server_id'")
            row = cur.fetchone()
            self.server_id = int(row[1])
            cur.close()

            cur = self.conn.cursor()
            cur.execute("select connection_id()")
            row = cur.fetchone()
            self.conn_id = row[0]
            cur.close()

        else:
            raise ValueError('engine error:' + engine)

class SQLiteConnection (DBConnection):
    type = "sqlite"

    def connect(self):
        engine = self.param['engine']
        self.trans = 0
        if engine == 'sqlite':
            import sqlite3
            self.conn = sqlite3.connect(self.param['db'], detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None, check_same_thread=False)
        else:
            raise ValueError('engine error:' + engine)

    def useit(self):
        DBConnection.useit(self)
        if not self.conn:
            self.connect()

    def releaseit(self):
        DBConnection.releaseit(self)
        self.conn.close()
        self.conn = None

    def escape(self, s):
        # simple escape TODO
        return s.replace("'", "''") \
            .replace('"', '\"')
            .replace(';', '')

    def last_insert_id(self):
        ret = self.query('select last_insert_rowid()', isdict=False)
        return ret[0][0]

    def begin(self):
        self.trans = 1
        sql = "BEGIN"
        return self.conn.execute(sql)



class DBPool (DBPoolBase):
    def __init__(self, dbcf):
        # one item: [conn, last_get_time, stauts]
        self.dbconn_idle  = []
        self.dbconn_using = []

        self.dbcf   = dbcf

        self.max_conn = 20
        self.min_conn = 1


        if 'conn' in self.dbcf:
            self.max_conn = self.dbcf['conn']

        self.connection_class = {}
        x = globals()
        for v in x.values():
            if isinstance(v, type) and v != DBConnection and issubclass(v, DBConnection):
                self.connection_class[v.type] = v

        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)

        self.open(self.min_conn)

    def synchronize(func):
        def _(self, *args, **argitems):
            self.lock.acquire()
            x = None
            try:
                x = func(self, *args, **argitems)
            finally:
                self.lock.release()
            return x
        return _

    def open(self, n=1):
        param = self.dbcf
        newconns = []
        for i in range(0, n):
            myconn = self.connection_class[param['engine']](param)
            myconn.pool = self
            newconns.append(myconn)
        self.dbconn_idle += newconns

    def clear_timeout(self):
        now = time.time()
        dels = []
        allconn = len(self.dbconn_idle)
        for c in self.dbconn_idle:
            if allconn == 1:
                break
            if now - c.lasttime > self.dbcf.get('idle_timeout', 10):
                dels.append(c)
                allconn -= 1

        if dels:
            log.debug('close timeout db conn:%d', len(dels))
            for c in dels:
                c.safe_close()
                self.dbconn_idle.remove(c)

    @synchronize
    def acquire(self, timeout=10):
        start = time.time()
        while len(self.dbconn_idle) == 0:
            if len(self.dbconn_idle) + len(self.dbconn_using) < self.max_conn:
                self.open()
                continue
            self.cond.wait(timeout)
            if int(time.time() - start) > timeout:
                log.error('func=acquire|error=no idle connections')
                raise RuntimeError('no idle connections')

        conn = self.dbconn_idle.pop(0)
        conn.useit()
        self.dbconn_using.append(conn)

        if random.randint(0, 100) > 80:
            try:
                self.clear_timeout()
            except:
                log.error(traceback.format_exc())

        return conn

    @synchronize
    def release(self, conn):
        if conn:
            if conn.trans:
                log.debug('release close conn use transaction')
                conn.safe_connect()

            self.dbconn_using.remove(conn)
            conn.releaseit()
            self.dbconn_idle.insert(0, conn)
        self.cond.notify()


    @synchronize
    def alive(self):
        for conn in self.dbconn_idle:
            conn.alive()

    def size(self):
        return len(self.dbconn_idle), len(self.dbconn_using)


class DBConnProxy:
    def __init__(self, rwpool, timeout=10):

        self._pool = rwpool
        self._master = None
        self._slave = None
        self._timeout = timeout

        self._modify_methods = set(['execute', 'executemany', 'last_insert_id',
                'insert', 'update', 'delete', 'insert_list', 'begin', 'rollback', 'commit'])

    def __getattr__(self, name):
        if name == 'master':
            if not self._master:
                self._master = self._pool.master.acquire(self._timeout)
            return self._master
        elif name == 'slave':
            if not self._slave:
                self._slave = self._pool.get_slave().acquire(self._timeout)
            return self._slave
        elif name in self._modify_methods:
            if not self._master:
                self._master = self._pool.master.acquire(self._timeout)
            return getattr(self._master, name)
        else:
            if not self._slave:
                self._slave = self._pool.get_slave().acquire(self._timeout)
            return getattr(self._slave, name)


class RWDBPool:
    def __init__(self, dbcf):
        self.dbcf   = dbcf
        self.policy = dbcf.get('policy', 'round_robin')

        self.master = DBPool(master_cf = dbcf.get('master', None))

        self.slaves = []
        self._slave_current = -1
        for x in dbcf.get('slave', []):
            slave = DBPool(x)
            self.slaves.append(slave)

    def get_slave(self):
        if self.policy == 'round_robin':
            size = len(self.slaves)
            self._slave_current = (self._slave_current + 1) % size
            return self.slaves[self._slave_current]
        else:
            raise ValueError('policy not support')

    def get_master(self):
        return self.master

    def acquire(self, timeout=10):
        return DBConnProxy(self, timeout)

    def release(self, conn):
        #log.debug('rwdbpool release')
        if conn._master:
            #log.debug('release master')
            conn._master.pool.release(conn._master)
        if conn._slave:
            #log.debug('release slave')
            conn._slave.pool.release(conn._slave)


    def size(self):
        ret = {'master': (-1,-1), 'slave':[]}
        if self.master:
            ret['master'] = self.master.size()
        for x in self.slaves:
            key = '%s@%s:%d' % (x.dbcf['user'], x.dbcf['host'], x.dbcf['port'])
            ret['slave'].append((key, x.size()))
        return ret

def checkalive(name=None):
    global dbpool
    while True:
        if name is None:
            checknames = dbpool.keys()
        else:
            checknames = [name]
        for k in checknames:
            pool = dbpool[k]
            pool.alive()
        time.sleep(300)

def install(cf):
    global dbpool
    if dbpool:
        log.warn("too many install db")
        return dbpool
    dbpool = {}

    for name,item in cf.items():
        item['name'] = name
        dbp = None
        if 'master' in item:
            dbp = RWDBPool(item)
        else:
            dbp = DBPool(item)
        dbpool[name] = dbp
    return dbpool


def acquire(name, timeout=10):
    global dbpool
    #log.info("acquire:", name)
    pool = dbpool[name]
    x = pool.acquire(timeout)
    x.name = name
    return x

def release(conn):
    if not conn:
        return
    global dbpool
    #log.info("release:", name)
    pool = dbpool[conn.name]
    return pool.release(conn)

@contextmanager
def get_connection(token, raise_except = True):
    conn = None
    try:
        conn = acquire(token)
        yield conn
    except:
        log.error("error=%s", traceback.format_exc())
        if raise_except:
            raise

    finally:
        if conn:
            release(conn)


def with_database(name, errfunc=None):
    def f(func):
        def _(self, *args, **argitems):
            self.db = acquire(name)
            x = None
            try:
                x = func(self, *args, **argitems)
            except Exception as e:
                if errfunc:
                    return getattr(self, errfunc)(error=e)
                else:
                    raise
            finally:
                release(self.db)
                self.db = None
            return x
        return _
    return f

def test():
    import random
    dbcf = {
        'test1': # connection name, used for getting connection from pool
            {
                'engine':'pymysql',      # db type, eg: mysql, sqlite
                'db':'test',       # db table
                'host':'172.100.101.156', # db host
                'port':3306,        # db port
                'user':'qf',      # db user
                'passwd':'123456',# db password
                'charset':'utf8',# db charset
                'conn':20, # max conn
            }
    }
    install(dbcf)

    log.debug('acquire')
    x = acquire('test1')
    log.debug('acquire ok')
    sql = 'drop table if exists user'
    x.execute(sql)
    sql = "create table if not exists user(id integer primary key, name varchar(32), ctime datetime)"
    x.execute(sql)

    #sql1 = "insert into user values (%d, 'zhaowei', datetime())" % (random.randint(1, 100));
    sql1 = "insert into user values (%d, 'zhaowei', now())" % (random.randint(1, 100));
    x.execute(sql1)

    x.insert("user", {
        "id": 123,
        "name":"bobo",
        "ctime":DBFunc("now()")
    })

    sql2 = "select * from user"
    ret = x.query(sql2)
    log.debug('result:%s', ret)

    log.debug('release')
    release(x)
    log.debug('release ok')

    log.debug('-' * 60)

    class Test2:
        @with_database("test1")
        def test2(self):
            ret = self.db.query("select * from user")
            log.debug(ret)

    t = Test2()
    t.test2()


def test1():
    DATABASE = {'test': # connection name, used for getting connection from pool
                {'engine':'pymysql',      # db type, eg: mysql, sqlite
                 'db':'test',       # db table
                 'host':'172.100.101.156', # db host
                 'port':3306,        # db port
                 'user':'qf',      # db user
                 'passwd':'123456',# db password
                 'charset':'utf8',# db charset
                 'conn':20}          # db connections in pool
           }

    install(DATABASE)

    while True:
        x = random.randint(0, 10)
        log.debug('x: %s', x)
        conns = []
        for i in range(0, x):
            c = acquire('test')
            time.sleep(1)
            conns.append(c)
            log.debug(dbpool['test'].size())

        for c in conns:
            release(c)
            time.sleep(1)
            log.debug(dbpool['test'].size())

        time.sleep(1)
        log.debug(dbpool['test'].size())

def test2():
    DATABASE = {'test': # connection name, used for getting connection from pool
                {'engine':'mysql',   # db type, eg: mysql, sqlite
                 'db':'test',        # db name
                 'host':'127.0.0.1', # db host
                 'port':3306,        # db port
                 'user':'root',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':110}          # db connections in pool
           }

    install(DATABASE)

    def go():
        #x = random.randint(0, 10)
        #print('x:', x
        #conns = []
        #for i in range(0, x):
        #    c = acquire('test')
        #    #time.sleep(1)
        #    conns.append(c)
        #    print(dbpool['test'].size()

        #for c in conns:
        #    release(c)
        #    #time.sleep(1)
        #    print(dbpool['test'].size()

        while True:
            #time.sleep(1)
            c = acquire('test')
            #print(dbpool['test'].size()
            release(c)
            #print(dbpool['test'].size()

    ths = []
    for i in range(0, 100):
        t = threading.Thread(target=go, args=())
        ths.append(t)

    for t in ths:
        t.start()

    for t in ths:
        t.join()


def test3():
    import logger
    logger.install('stdout')
    global log
    log = logger.log

    DATABASE = {'test':{
                'policy': 'round_robin',
                'default_conn':'auto',
                'master':
                    {'engine':'mysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'root',
                     'passwd':'123456',
                     'charset':'utf8',
                     'idle_timeout':60,
                     'conn':20},
                'slave':[
                    {'engine':'mysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'jove',
                     'passwd':'123456',
                     'charset':'utf8',
                     'conn':20},
                    {'engine':'mysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'jove',
                     'passwd':'123456',
                     'charset':'utf8',
                     'conn':20},
                    ],
                },

           }

    install(DATABASE)

    while True:
        x = random.randint(0, 10)
        print('x:', x)
        conns = []

        print('acquire ...')
        for i in range(0, x):
            c = acquire('test')
            time.sleep(1)
            c.insert('ztest', {'name':'jove%d'%(i)})
            print(c.query('select count(*) from ztest'))
            print(c.get('select count(*) from ztest'))
            conns.append(c)
            print(dbpool['test'].size())


        print('release ...')
        for c in conns:
            release(c)
            time.sleep(1)
            print(dbpool['test'].size())

        time.sleep(1)
        print('-'*60)
        print(dbpool['test'].size())
        print('-'*60)
        time.sleep(1)

def test4(tcount):
    DATABASE = {'test': # connection name, used for getting connection from pool
                {'engine':'mysql',   # db type, eg: mysql, sqlite
                 'db':'test',        # db name
                 'host':'127.0.0.1', # db host
                 'port':3306,        # db port
                 'user':'jove',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':10}          # db connections in pool
           }

    install(DATABASE)

    def run_thread():
        while True:
            time.sleep(0.01)
            conn = None
            try:
                conn = acquire('test')
            except:
                log.debug("%s catch exception in acquire", threading.currentThread().name)
                traceback.print_exc()
                time.sleep(0.5)
                continue
            try:
                sql = "select count(*) from profile"
                ret = conn.query(sql)
            except:
                log.debug("%s catch exception in query", threading.currentThread().name)
                traceback.print_exc()
            finally:
                if conn:
                    release(conn)
                    conn = None

    import threading
    th = []
    for i in range(0, tcount):
        _th = threading.Thread(target=run_thread, args=())
        log.debug("%s create", _th.name)
        th.append(_th)

    for t in th:
        t.start()
        log.debug("%s start", t.name)

    for t in th:
        t.join()
        log.debug("%s finish",t.name)


def test5():
    DATABASE = {'test': # connection name, used for getting connection from pool
                {'engine':'mysql',   # db type, eg: mysql, sqlite
                 'db':'test',        # db name
                 'host':'127.0.0.1', # db host
                 'port':3306,        # db port
                 'user':'jove',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':20}          # db connections in pool
           }

    install(DATABASE)

    def run_thread():
        i = 0
        while i < 10:
            time.sleep(0.01)
            with get_connection('test') as conn:
                sql = "select count(*) from profile"
                ret = conn.query(sql)
                log.debug('ret:%s', ret)
            i += 1
        pool = dbpool['test']
        log.debug("pool size: %s", pool.size())
    import threading
    th = []
    for i in range(0, 10):
        _th = threading.Thread(target=run_thread, args=())
        log.debug("%s create", _th.name)
        th.append(_th)

    for t in th:
        t.setDaemon(True)
        t.start()
        log.debug("%s start", t.name)

def test_with():
    DATABASE = {'test': # connection name, used for getting connection from pool
                {'engine':'pymysql',   # db type, eg: mysql, sqlite
                 'db':'qf_core',        # db name
                 'host':'127.0.0.1', # db host
                 'port':3306,        # db port
                 'user':'jove',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':10}          # db connections in pool
           }

    install(DATABASE)
    with get_connection('test') as conn:
        record = conn.query("select nickname from profile where userid=227519")
        print(record)
        #record = conn.query("select * from chnlbind where userid=227519")
        #print(record
    pool = dbpool['test']
    print(pool.size())

    with get_connection('test') as conn:
        record = conn.query("select * from profile where userid=227519")
        print(record)
        record = conn.query("select * from chnlbind where userid=227519")
        print(record)

    pool = dbpool['test']
    print(pool.size())

def test_base_func():
    import logger
    logger.install('stdout')
    database = {'test': # connection name, used for getting connection from pool
                {'engine':'mysql',   # db type, eg: mysql, sqlite
                 'db':'qf_core',        # db name
                 'host':'172.100.101.151', # db host
                 'port':3306,        # db port
                 'user':'qf',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':10}          # db connections in pool
           }
    install(database)
    with get_connection('test') as conn:
        conn.insert('auth_user',{
            'username':'13512345677',
            'password':'123',
            'mobile':'13512345677',
            'email':'123@qfpay.cn',
        })
        print( conn.select('auth_user',{
            'username':'13512345677',
        }))
        conn.delete('auth_user',{
            'username':'13512345677',
        })
        conn.select_join('profile as p','auth_user as a',where={
            'p.userid':DBFunc('a.id'),
        })

def test_new_rw():
    import logger
    logger.install('stdout')
    database = {'test':{
                'policy': 'round_robin',
                'default_conn':'auto',
                'master':
                    {'engine':'pymysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'jove',
                     'passwd':'123456',
                     'charset':'utf8',
                     'conn':10}
                 ,
                 'slave':[
                    {'engine':'pymysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'jove',
                     'passwd':'123456',
                     'charset':'utf8',
                     'conn':10
                    },
                    {'engine':'pymysql',
                     'db':'test',
                     'host':'127.0.0.1',
                     'port':3306,
                     'user':'jove',
                     'passwd':'123456',
                     'charset':'utf8',
                     'conn':10
                    }

                 ]
             }
            }
    install(database)

    def printt(t=0):
        now = time.time()
        if t > 0:
            print('time:', now-t)
        return now

    t = printt()
    with get_connection('test') as conn:
        t = printt(t)
        print('master:', conn._master, 'slave:', conn._slave)
        assert conn._master == None
        assert conn._slave == None
        ret = conn.query("select 10")
        t = printt(t)
        print('after read master:', conn._master, 'slave:', conn._slave)
        assert conn._master == None
        assert conn._slave != None
        conn.execute('create table if not exists haha (id int(4) not null primary key, name varchar(128) not null)')
        t = printt(t)
        print('master:', conn._master, 'slave:', conn._slave)
        assert conn._master != None
        assert conn._slave != None
        conn.execute('drop table haha')
        t = printt(t)
        assert conn._master != None
        assert conn._slave != None
        print('ok')

    print('=' * 20)
    t = printt()
    with get_connection('test') as conn:
        t = printt(t)
        print('master:', conn._master, 'slave:', conn._slave)
        assert conn._master == None
        assert conn._slave == None

        ret = conn.master.query("select 10")
        assert conn._master != None
        assert conn._slave == None

        t = printt(t)
        print('after query master:', conn._master, 'slave:', conn._slave)
        ret = conn.query("select 10")
        assert conn._master != None
        assert conn._slave != None

        print('after query master:', conn._master, 'slave:', conn._slave)
        print('ok')

def test_db_install():
    import logger
    logger.install('stdout')
    DATABASE = {'test': # connection name, used for getting connection from pool
                {'engine':'pymysql',   # db type, eg: mysql, sqlite
                 'db':'test',        # db name
                 'host':'127.0.0.1', # db host
                 'port':3306,        # db port
                 'user':'jove',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':10}          # db connections in pool
           }

    install(DATABASE)
    install(DATABASE)
    install(DATABASE)
    install(DATABASE)

    with get_connection('test') as conn:
        for i in range(0, 100):
            print(conn.select_one('order'))

def test_trans():
    import logger
    logger.install('stdout')
    DATABASE = {'test': # connection name, used for getting connection from pool
                {'engine':'pymysql',   # db type, eg: mysql, sqlite
                 'db':'test',        # db name
                 'host':'127.0.0.1', # db host
                 'port':3306,        # db port
                 'user':'jove',      # db user
                 'passwd':'123456',  # db password
                 'charset':'utf8',   # db charset
                 'conn':10}          # db connections in pool
           }

    install(DATABASE)

    with get_connection('test') as conn:
        conn.start()
        conn.select_one('order')
        conn.get('select connection_id()')

        conn.select_one('order')

    with get_connection('test') as conn:
        conn.select_one('order')
        conn.get('select connection_id()')


def test_sqlite_escape():
    database = {
        'test': {
            'engine': 'sqlite',
            'db':'test.db',
        }
    }
    install(database)
    with get_connection('test') as conn:
        conn.execute('drop table haha')
        conn.execute('create table if not exists haha (id int(4) not null primary key, name varchar(128) not null)')
        conn.insert('haha', {
            'id': 1,
            'name': "'",
        })
        conn.select('haha', {
            'name': "'",
        })
        conn.insert('haha', {
            'id': 2,
            'name': '"',
        })
        conn.select('haha', {
            'name': '"',
        })

if __name__ == '__main__':
    import logger
    logger.install('stdout')
    # test()
    # test1()
    #  test_with()
    test_sqlite_escape()
    print('complete!')



