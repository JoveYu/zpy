# coding: utf-8

import sys
import logging
import logging.config
from logging import DEBUG, INFO, WARN, ERROR, FATAL, NOTSET

LEVEL_COLOR = {
    DEBUG: '\33[37m',
    INFO: '\33[36m',
    WARN: '\33[33m',
    ERROR: '\33[35m',
    FATAL: '\33[31m',
    NOTSET: ''
}

log = None

class ScreenHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            fs = LEVEL_COLOR[record.levelno] + "%s\n" + '\33[0m'
            self.stream.write(fs % msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

logging.ScreenHandler = ScreenHandler

def debug(msg, *args, **kwargs):
    global log
    log.debug(msg, *args, **kwargs)

def info(msg, *args, **kwargs):
    global log
    log.info(msg, *args, **kwargs)

def warn(msg, *args, **kwargs):
    global log
    log.warn(msg, *args, **kwargs)
warning = warn

def error(msg, *args, **kwargs):
    global log
    log.error(msg, *args, **kwargs)

def fatal(msg, *args, **kwargs):
    global log
    log.fatal(msg, *args, **kwargs)
critical = fatal

def install(logdict, **options):
    pyv = sys.version_info
    if pyv[0] == 2 and pyv[1] < 7:
        raise RuntimeError('python error, must python >= 2.7')

    conf = {
        'version': 1,
        'formatters': {
            'myformat': {
                'format': '%(asctime)s %(process)d,%(threadName)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.ScreenHandler',
                'formatter': 'myformat',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            },
        },
        'loggers': {
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['console'],
        }
    }

    # 如果是传递文件进来 变成level 字典
    if isinstance(logdict, str):
        logdict = {'DEBUG': logdict}

    # 如果是单handler 直接传递文件进来
    if 'root' not in logdict:
        logdict = {
            'root':{
                'filename':logdict,
            }
        }
        if options:
            logdict['root'].update(options)

    # 构建handler 和logger
    for logname,logcf in logdict.items():
        filehandlers = []
        filename = logcf.pop('filename')
        for level,name in filename.items():
            if name != 'stdout':
                conf['handlers']['file-'+name] = {
                    'class': 'logging.handlers.WatchedFileHandler',
                    'formatter': 'myformat',
                    'level': level.upper(),
                    'filename': name,
                }
                if logcf:
                    if 'when' in logcf:
                        logcf['class'] = 'logging.handlers.TimedRotatingFileHandler'
                    conf['handlers']['file-'+name].update(logcf)

                filehandlers.append('file-'+name)
        if filehandlers:
            conf['loggers'][logname] = {'handlers': filehandlers}

    # 把 root 从logger 里面移出来
    if 'root' in conf['loggers']:
        conf['root'] = conf['loggers'].pop('root')
        conf['root']['level'] = 'DEBUG'

    # 调整 propagate
    for logname in logdict:
        if logname != 'root':
            logobj = logging.getLogger(logname)
            logobj.propagate = False

    logging.config.dictConfig(conf)
    global log
    log = logging.getLogger()
    return log


def test1():
    install('stdout')
    log = logging.getLogger()
    for i in range(0, 5):
        log.debug('debug ... %d', i)
        log.info('info ... %d', i)
        log.warn('warn ... %d', i)
        log.error('error ... %d', i)
        log.fatal('fatal ... %d', i)

def test2():
    install({
        'DEBUG':'test.log',
        'WARN':'test.warn.log'
    })
    log = logging.getLogger()
    for i in range(0, 5):
        log.debug('debug ... %d', i)
        log.info('info ... %d', i)
        log.warn('warn ... %d', i)
        log.error('error ... %d', i)
        log.fatal('fatal ... %d', i)

def test3():
    import time
    install({
        'DEBUG':'test.log',
        'WARN':'test.warn.log'
    }, when = 'S', backupCount=3)
    log = logging.getLogger()
    for i in range(0, 5):
        log.debug('debug ... %d', i)
        log.info('info ... %d', i)
        log.warn('warn ... %d', i)
        log.error('error ... %d', i)
        log.fatal('fatal ... %d', i)
        time.sleep(1)


def test4():
    import time
    install({
        'root':{
            'filename':{
                'DEBUG':'test.log',
                'WARN':'test.warn.log'
            },
        },
        'logger1':{
            'filename':{
                'DEBUG':'test1.log',
                'WARN':'test1.warn.log'
            },
            'when': 'S',
        }
    })
    log = logging.getLogger()
    for i in range(0, 5):
        log.debug('debug ... %d', i)
        log.info('info ... %d', i)
        log.warn('warn ... %d', i)
        log.error('error ... %d', i)
        log.fatal('fatal ... %d', i)

    log = logging.getLogger('logger1')
    for i in range(0, 5):
        log.debug('debug ... %d', i)
        log.info('info ... %d', i)
        log.warn('warn ... %d', i)
        log.error('error ... %d', i)
        log.fatal('fatal ... %d', i)
        time.sleep(1)

def test5():
    install({
        'root': {
            'filename': {
                'DEBUG': 'stdout'
            },
        },
        'logger1': {
            'filename': {
                'DEBUG':"test.log",
                'ERROR':'test.err.log'
            },
        },
    })

    log1 = logging.getLogger()
    for i in range(0, 5):
        log1.debug('debug ... %d', i)
        log1.info('info ... %d', i)
        log1.warn('warn ... %d', i)
        log1.error('error ... %d', i)
        log1.fatal('fatal ... %d', i)

    log2 = logging.getLogger('logger1')
    for i in range(0, 5):
        log2.debug('debug ... %d', i)
        log2.info('info ... %d', i)
        log2.warn('warn ... %d', i)
        log2.error('error ... %d', i)
        log2.fatal('fatal ... %d', i)



if __name__ == '__main__':
    test1()
    # test2()
    # test3()
    # test4()
    # test5()
