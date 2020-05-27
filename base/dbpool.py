
# ref:
#   PEP 248 https://www.python.org/dev/peps/pep-0248/
#   PEP 249 https://www.python.org/dev/peps/pep-0249/
#   wiki    https://wiki.python.org/moin/DatabaseProgramming

import time
import datetime
import types
import random
import logging
import traceback
import functools
import asyncio
from contextlib import asynccontextmanager

log = logging.getLogger()

dbpool = None

def timeit(func):
    @functools.wraps(func)
    async def _(conn, *args, **kwargs):
        starttm = time.time()
        ret = 0
        err = ''
        try:
            retval = await func(conn, *args, **kwargs)
            if isinstance(retval, list):
                ret = len(retval)
            elif isinstance(retval, dict):
                ret = 1
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
            log.info('server=%s|name=%s|addr=%s@%s:%d/%s|idle=%d|busy=%d|max=%d|time=%d|ret=%s|sql=%s|err=%s',
                     conn.type, conn.name,
                     dbcf.get('user', ''), dbcf.get('host', ''),
                     dbcf.get('port', 0), dbcf.get('db', ''),
                     len(conn.pool.dbconn_idle),
                     len(conn.pool.dbconn_using),
                     conn.pool.max_conn,
                     int((endtm-starttm)*1000000),
                     ret, args[0], err)

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
    type = ''

    def __init__(self, param):
        self.param = param
        self.name = param.get('name', '')
        self.pool = None
        self.conn = None
        self.status = 0
        self.server_id = 0
        self.conn_id = 0
        self.lasttime = time.time()

    def __str__(self):
        return '<{} {}@{}:{:d}/{}>'.format(
            self.__class__.__name__,
            self.param.get('user', ''),
            self.param.get('host', ''),
            self.param.get('port', 0),
            self.param.get('db', '')
        )

    async def _close_conn(self, conn):
        ret = conn.close()
        if asyncio.iscoroutine(ret):
            return await ret
        else:
            return ret

    async def _close_cursor(self, cur):
        ret = cur.close()
        if asyncio.iscoroutine(ret):
            return await ret
        else:
            return ret

    def is_available(self):
        return self.status == 0

    async def useit(self):
        self.status = 1
        self.lasttime = time.time()

    async def releaseit(self):
        self.status = 0

    async def connect(self):
        raise NotImplementedError

    async def close(self):
        await self._close_conn(self.conn)
        self.conn = None

    async def reconnect(self):
        try:
            await self.close()
            await self.connect()
        except:
            log.error(traceback.format_exc())

    async def alive(self):
        raise NotImplementedError

    async def last_insert_id(self):
        raise NotImplementedError

    async def begin(self):
        await self.conn.begin()

    async def commit(self):
        await self.conn.commit()

    async def rollback(self):
        await self.conn.rollback()

    @asynccontextmanager
    async def transaction(self):
        try:
            await self.begin()
            yield
            await self.commit()
        except Exception as e:
            await self.rollback()
            raise e

    def escape(self, s):
        return s

    async def cursor(self):
        return await self.conn.cursor()

    @timeit
    async def execute(self, sql, param=None):
        cur = await self.conn.cursor()
        try:
            if param:
                ret = await cur.execute(sql, param)
            else:
                ret = await cur.execute(sql)
            return ret
        finally:
            await self._close_cursor(cur)

    @timeit
    async def executemany(self, sql, param):
        cur = await self.conn.cursor()
        try:
            if param:
                ret = await cur.executemany(sql, param)
            else:
                ret = await cur.executemany(sql)
            return ret
        finally:
            await self._close_cursor(cur)

    @timeit
    async def query(self, sql, param=None, isdict=True, head=False):
        cur = await self.conn.cursor()
        try:
            if param:
                await cur.execute(sql, param)
            else:
                await cur.execute(sql)
            res = await cur.fetchall()
        finally:
            await self._close_cursor(cur)
        if res and isdict:
            ret = []
            xkeys = [i[0] for i in cur.description]
            for item in res:
                ret.append(dict(zip(xkeys, item)))
        else:
            ret = res
            if head:
                xkeys = [i[0] for i in cur.description]
                ret.insert(0, xkeys)
        return ret

    @timeit
    async def get(self, sql, param=None, isdict=True):
        cur = await self.conn.cursor()
        try:
            if param:
                await cur.execute(sql, param)
            else:
                await cur.execute(sql)
            res = await cur.fetchone()
        finally:
            await self._close_cursor(cur)
        if res and isdict:
            xkeys = [i[0] for i in cur.description]
            return dict(zip(xkeys, res))
        else:
            return res

    def value2sql(self, v):
        if isinstance(v, str):
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
        item = '("%s" %s ' % (key, op)
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
                x.append(self.exp2sql(k, v[0], v[1]))
            else:
                x.append('"%s"=%s' % (k, self.value2sql(v)))
        return sp.join(x)

    def dict2insert(self, d):
        keys = []
        vals = []
        for k in sorted(d.keys()):
            vals.append('%s' % self.value2sql(d[k]))
            keys.append('"%s"' % k)
        return ','.join(keys), ','.join(vals)

    async def insert(self, table, values, other=None):
        sql = self.insert_sql(table, values, other=other)
        return await self.execute(sql)

    async def insertmany(self, table, values_list, other=None):
        sql = self.insertmany_sql(table, values_list, other=other)
        return await self.execute(sql)

    async def update(self, table, values, where=None, other=None):
        sql = self.update_sql(table, values, where=where, other=other)
        return await self.execute(sql)

    async def delete(self, table, where, other=None):
        sql = self.delete_sql(table, where, other=other)
        return await self.execute(sql)

    async def select(self, table, where=None, fields='*', other=None, isdict=True):
        sql = self.select_sql(table, where, fields, other)
        return await self.query(sql, None, isdict=isdict)

    async def select_one(self, table, where=None, fields='*', other='limit 1', isdict=True):
        sql = self.select_sql(table, where, fields, other)
        return await self.get(sql, None, isdict=isdict)

    async def select_join(self, table1, table2, join_type='inner', on=None, where=None, fields='*', other=None, isdict=True):
        sql = self.select_join_sql(table1, table2, join_type, on, where, fields, other)
        return await self.query(sql, None, isdict=isdict)

    async def select_join_one(self, table1, table2, join_type='inner', on=None, where=None, fields='*', other='limit 1', isdict=True):
        sql = self.select_join_sql(table1, table2, join_type, on, where, fields, other)
        return await self.get(sql, None, isdict=isdict)

    def insert_sql(self, table, values, other=None):
        keys, vals = self.dict2insert(values)
        sql = 'insert into "%s"(%s) values (%s)' % (table, keys, vals)
        if other:
            sql += ' %s' % other
        return sql

    def insertmany_sql(self, table, values_list, other=None):
        vals_list = []
        for values in values_list:
            keys, vals = self.dict2insert(values)
            vals_list.append('(%s)' % vals)

        sql = 'insert into "%s"(%s) values %s' % (table, keys, ','.join(vals_list))
        if other:
            sql += ' %s' % other
        return sql

    def update_sql(self, table, values, where=None, other=None):
        sql = 'update "%s" set %s' % (table, self.dict2sql(values))
        if where:
            sql += ' where %s' % self.dict2sql(where,' and ')
        if other:
            sql += ' %s' % other
        return sql

    def delete_sql(self, table, where, other=None):
        sql = 'delete from "%s"' % table
        if where:
            sql += ' where %s' % self.dict2sql(where, ' and ')
        if other:
            sql += ' %s' % other
        return sql

    def select_sql(self, table, where=None, fields='*', other=None):
        if isinstance(fields, (list, tuple)):
            fields = ','.join(fields)
        sql = 'select %s from "%s"' % (fields, table)
        if where:
            sql += ' where %s' % self.dict2sql(where, ' and ')
        if other:
            sql += ' %s' % other
        return sql

    def select_join_sql(self, table1, table2, join_type='inner', on=None, where=None, fields='*', other=None):
        if isinstance(fields, (list, tuple)):
            fields = ','.join(fields)
        sql = 'select %s from "%s" %s join "%s"' % (fields, table1, join_type, table2)
        if on:
            sql += ' on %s' % on
        if where:
            sql += ' where %s' % self.dict2sql(where, ' and ')
        if other:
            sql += ' %s' % other
        return sql

def with_mysql_reconnect(func):

    @functools.wraps(func)
    async def _(self, *args, **argitems):
        import aiomysql
        trycount = 3
        while True:
            try:
                x = await func(self, *args, **argitems)
            except aiomysql.OperationalError as e:
                if e.args[0] >= 2000:  # client error
                    await self.reconnect()
                    trycount -= 1
                    if trycount > 0:
                        continue
                raise
            except (aiomysql.InterfaceError, aiomysql.InternalError):
                await self.reconnect()
                raise
            else:
                return x
    return _

class AIOMySQLConnection(DBConnection):
    type = "mysql"

    async def connect(self):
        import aiomysql
        self.conn = await aiomysql.connect(host=self.param['host'],
                                    port=self.param['port'],
                                    user=self.param['user'],
                                    password=self.param['passwd'],
                                    db=self.param['db'],
                                    charset=self.param['charset'],
                                    connect_timeout=self.param.get(
                                        'timeout', 10),
                                    sql_mode="ANSI_QUOTES", # allow " replace `
                                    autocommit=True,
                                    )

        row = await self.get("show variables like 'server_id'", isdict=False)
        self.server_id = int(row[1])

        row = await self.get("select connection_id()", isdict=False)
        self.conn_id = row[0]

    @with_mysql_reconnect
    async def alive(self):
        await self.conn.ping()

    def escape(self, s):
        return self.conn.escape_string(s)

    async def last_insert_id(self):
        ret = await self.get('select last_insert_id()', isdict=False)
        return ret[0]

    @with_mysql_reconnect
    async def execute(self, sql, param=None):
        return await DBConnection.execute(self, sql, param)

    @with_mysql_reconnect
    async def executemany(self, sql, param):
        return await DBConnection.executemany(self, sql, param)

    @with_mysql_reconnect
    async def query(self, sql, param=None, isdict=True, head=False):
        return await DBConnection.query(self, sql, param, isdict, head)

    @with_mysql_reconnect
    async def get(self, sql, param=None, isdict=True):
        return await DBConnection.get(self, sql, param, isdict)


class AIOSQLiteConnection (DBConnection):
    type = "sqlite"

    async def connect(self):
        import aiosqlite
        self.conn = await aiosqlite.connect(self.param['db'], isolation_level=None)

    async def useit(self):
        await DBConnection.useit(self)
        if not self.conn:
            await self.connect()

    async def releaseit(self):
        await DBConnection.releaseit(self)
        await self.close()

    def escape(self, s):
        # simple escape TODO
        return s.replace("'", "''")

    def last_insert_id(self):
        ret = self.get('select last_insert_rowid()', isdict=False)
        return ret[0]

    def begin(self):
        return self.execute('begin')

class AIOPGConnection(DBConnection):
    type = 'postgresql'

    async def connect(self):
        import aiopg
        self.conn = await aiopg.connect(host=self.param['host'],
                                    port=self.param['port'],
                                    user=self.param['user'],
                                    password=self.param['passwd'],
                                    database=self.param['db'],
                                    )

    async def alive(self):
        await self.conn.ping()

    def escape(self, s):
        # simple escape TODO
        return s.replace("'", "''")

    async def last_insert_id(self):
        ret = await self.get('select lastval()', isdict=False)
        return ret[0]

    async def begin(self):
        return await self.execute('start transaction')



def synchronize(func):
    @functools.wraps(func)
    async def _(self, *args, **kwargs):
        await self.lock.acquire()
        x = None
        try:
            x = await func(self, *args, **kwargs)
        finally:
            self.lock.release()
        return x
    return _

class DBPool (DBPoolBase):
    def __init__(self, dbcf):
        self.dbconn_idle = []
        self.dbconn_using = []

        self.dbcf = dbcf

        self.max_conn = dbcf.get('conn',50)
        self.min_conn = 1

        self.connection_class = {}
        x = globals()
        for v in x.values():
            if isinstance(v, type) and v != DBConnection and issubclass(v, DBConnection):
                self.connection_class[v.type] = v

        self.lock = asyncio.Lock()
        self.cond = asyncio.Condition(self.lock)


    async def open(self, n=1):
        param = self.dbcf
        newconns = []
        for _ in range(0, n):
            myconn = self.connection_class[param['engine']](param)
            myconn.pool = self
            await myconn.connect()
            newconns.append(myconn)
        self.dbconn_idle += newconns

    async def clear_idle(self):
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
                await c.close()
                self.dbconn_idle.remove(c)

    @synchronize
    async def acquire(self, timeout=5):
        start = time.time()
        while len(self.dbconn_idle) == 0:
            if len(self.dbconn_idle) + len(self.dbconn_using) < self.max_conn:
                await self.open()
                continue
            await asyncio.wait_for(self.cond.wait(), timeout)
            if int(time.time() - start) > timeout:
                log.error('func=acquire|error=no idle connections')
                raise RuntimeError('no idle connections')

        conn = self.dbconn_idle.pop(0)
        await conn.useit()
        self.dbconn_using.append(conn)

        if random.randint(0, 100) > 80:
            try:
                await self.clear_idle()
            except:
                log.error(traceback.format_exc())

        return conn

    @synchronize
    async def release(self, conn):
        self.dbconn_using.remove(conn)
        await conn.releaseit()
        self.dbconn_idle.insert(0, conn)
        self.cond.notify()

    @synchronize
    async def alive(self):
        for conn in self.dbconn_idle:
            await conn.alive()

    def size(self):
        return len(self.dbconn_idle), len(self.dbconn_using)


async def checkalive(name=None, sleep=300):
    global dbpool
    while True:
        if name is None:
            checknames = dbpool.keys()
        else:
            checknames = [name]
        for k in checknames:
            pool = dbpool[k]
            await pool.alive()
        await asyncio.sleep(sleep)


def install(cf):
    global dbpool
    if dbpool:
        log.warning("too many install db")
        return dbpool
    dbpool = {}

    for name, item in cf.items():
        item['name'] = name
        dbp = DBPool(item)
        dbpool[name] = dbp
    return dbpool


async def acquire(name, timeout=5):
    global dbpool
    pool = dbpool[name]
    return await pool.acquire(timeout)


async def release(conn):
    if not conn:
        return
    global dbpool
    pool = dbpool[conn.name]
    return await pool.release(conn)


@asynccontextmanager
async def get_connection(name):
    conn = None
    try:
        conn = await acquire(name)
        yield conn
    finally:
        if conn:
            await release(conn)

def with_database(name):
    def f(func):
        @functools.wraps(func)
        async def _(self, *args, **kwargs):
            if getattr(self, 'db', None) is not None:
                raise RuntimeError('acquire database duplicate')
            if isinstance(name, list):
                self.db = {}
                for i in name:
                    self.db[i] = await acquire(i)
                try:
                    return await func(self, *args, **kwargs)
                finally:
                    for _,i in self.db.items():
                        await release(i)
                    self.db = None
            else:
                self.db = await acquire(name)
                try:
                    return await func(self, *args, **kwargs)
                finally:
                    await release(self.db)
                    self.db = None
        return _
    return f












test_dbcf = {
    'mysql':{
        'engine': 'mysql',
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'passwd': '123456',
        'db': 'test',
        'charset': 'utf8',
    },
    'pg':{
        'engine': 'postgresql',
        'host': '127.0.0.1',
        'port': 5432,
        'user': 'postgres',
        'passwd': '123456',
        'db': 'test',
    },
    'sqlite':{
        'engine': 'sqlite',
        'db': 'test.db',
    }
}

async def test_with():
    install(test_dbcf)

    class Class1:
        @with_database('mysql')
        async def f1(self):
            await self.db.query('show tables')

        @with_database(['mysql'])
        async def f2(self):
            # await self.f1()   # raise
            await self.db['mysql'].query('show tables')

    c = Class1()
    await c.f1()
    await c.f2()

async def test_pg_conn():
    install(test_dbcf)

    async with get_connection('pg') as db:
        await db.execute('drop table if exists test')
        await db.execute('''
        create table if not exists test(
            id SERIAL,
            name varchar(30),
            time timestamp
        )
        ''')
        await db.insert('test', {
            'name': '杰克\'马"',
            'time': datetime.datetime.now(),
        })
        ret = await db.last_insert_id()
        log.debug(ret)
        ret = await db.select('test', {
            'id': 1
        })
        log.debug(ret)
        await db.delete('test', {
            'name': 'jack'
        })
        ret = await db.select('test')
        log.debug(ret)

async def test_sqlite_conn():
    install(test_dbcf)

    async with get_connection('sqlite') as db:
        await db.execute('drop table if exists test')
        await db.execute('''
        create table if not exists test(
            id int,
            name varchar(30),
            time datetime
        )
        ''')
        await db.insert('test', {
            'id': 1,
            'name': '杰克\'马"',
            'time': datetime.datetime.now(),
        })
        ret = await db.select('test', {
            'id': 1
        })
        log.debug(ret)
        await db.delete('test', {
            'name': 'jack'
        })
        ret = await db.select('test')
        log.debug(ret)

async def test_mysql_conn():
    install(test_dbcf)

    async with get_connection('mysql') as db:
        await db.execute('drop table if exists test')
        await db.execute('''
        create table if not exists test(
            id int,
            name varchar(30),
            time datetime
        )ENGINE=InnoDB DEFAULT CHARSET=utf8
        ''')
        await db.insert('test', {
            'id': 1,
            'name': '杰克\'马"',
            'time': datetime.datetime.now(),
        })
        await db.insertmany('test', [
            {
                'id': 2,
                'name': '杰克马',
                'time': datetime.datetime.now(),
            },
            {
                'id': 3,
                'name': 'jack',
                'time': datetime.datetime.now(),
            },
        ])
        ret = await db.select('test', {
            'id': ('>', 1),
        })
        log.debug(ret)

        await db.update('test', {
            'name': '新名字',
            'time': datetime.datetime.now(),
        }, {
            'id': 2
        })
        ret = await db.select('test', {
            'id': 2
        })
        log.debug(ret)

        await db.delete('test', {
            'name': 'jack'
        })
        ret = await db.select('test')
        log.debug(ret)

        ret = await db.select_one('test', fields='count(1)', isdict=False)
        log.debug(ret)

        async with db.transaction():
            pass


async def test_mysql_reconnect():
    install(test_dbcf)

    async with get_connection('mysql') as db:
        while 1:
            ret = await db.query('select 1')
            log.debug(ret)
            await asyncio.sleep(1)

async def test_max_conn():
    install(test_dbcf)

    import random
    async def task(i):
        while 1:
            try:
                async with get_connection('mysql') as db:
                    await db.query('select sleep(%f)' % (random.randint(0,1000)/1000))
                await asyncio.sleep(random.randint(0,2000)/1000)
            except:
                log.error(traceback.format_exc())

    loop = asyncio.get_event_loop()
    for i in range(30):
        loop.create_task(task(i))
    await asyncio.sleep(60)

if __name__ == '__main__':
    import logger
    logger.install('stdout')
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(test_with())
    # loop.run_until_complete(test_mysql_conn())
    # loop.run_until_complete(test_sqlite_conn())
    # loop.run_until_complete(test_pg_conn())
    # loop.run_until_complete(test_mysql_reconnect())
    loop.run_until_complete(test_max_conn())





