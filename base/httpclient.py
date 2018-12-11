#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ssl
import json
import os
import logging
import types
import urllib
import urllib.request
import http
import time
import io
import functools

log = logging.getLogger()

client = None
conn_pool = None

def timeit(func):
    @functools.wraps(func)
    def _(self, *args, **kwargs):
        starttm = time.time()
        code = 0
        content = ''
        err = ''
        try:
            retval = func(self, *args, **kwargs)
            code = self.code
            if b'\0' in self.content:
                content = '[binary data %d]' % len(self.content)
            else:
                if self._charset:
                    self.content = self.content.decode(self._charset)
                    retval = retval.decode(self._charset)
                content = self.content[:2000]
            return retval
        except Exception as e:
            err = str(e)
            raise
        finally:
            endtm = time.time()
            log.info('server=HTTPClient|name=%s|func=%s|code=%s|time=%d|args=%s|kwargs=%s|err=%s|content=%s',
                     self.name,func.__name__,str(code),
                     int((endtm-starttm)*1000000),
                     str(args),str(kwargs),
                     err,content)
    return _

def install(name, **kwargs):
    global client
    x = globals()
    for k in x.keys():
        v = x[k]
        if type(v) == types.ClassType and v != HTTPClient and issubclass(v, HTTPClient):
            if v.name == name:
                client = v(**kwargs)
                return client

def dict2xml(root, sep='', cdata=True):
    '''sep 可以为 \n'''
    xml = ''
    for key in sorted(root.keys()):
        if isinstance(root[key], dict):
            xml = '%s<%s>%s%s</%s>%s' % (xml, key, sep, dict2xml(root[key], sep), key, sep)
        elif isinstance(root[key], list):
            xml = '%s<%s>' % (xml, key)
            for item in root[key]:
                xml = '%s%s' % (xml, dict2xml(item,sep))
            xml = '%s</%s>' % (xml, key)
        else:
            value = root[key]

            if cdata:
                xml = '%s<%s><![CDATA[%s]]></%s>%s' % (xml, key, value, key, sep)
            else:
                xml = '%s<%s>%s</%s>%s' % (xml, key, value, key, sep)
    return xml


class HTTPClient:
    code = 0
    content = ''
    header = {}

    def __init__(self, verify_ssl_certs=True, timeout=10, conn_pool=False, allow_redirect=False, charset='utf-8'):
        self._verify_ssl_certs = verify_ssl_certs
        self._timeout = timeout
        self._conn_pool = conn_pool
        self._allow_redirect = allow_redirect
        self._charset = charset

    @timeit
    def do(self, method, url, header={}, post_data=None):
        content, code, header = self.request(self, method, url, header, post_data)
        return content

    @timeit
    def get(self, url, params={}, header={}, **kwargs):
        if params:
            if '?' in url:
                url = url + '&' + urllib.parse.urlencode(params)
            else:
                url = url + '?' + urllib.parse.urlencode(params)

        content, code, header = self.request('get', url, header, **kwargs)

        return content

    @timeit
    def post(self, url, params={}, header={}, **kwargs):

        header['Content-Type'] = 'application/x-www-form-urlencoded'
        post_data = urllib.parse.urlencode(params)

        content, code, header = self.request('post', url, header, post_data, **kwargs)
        return content

    @timeit
    def post_json(self, url, json_dict={}, header={}, json_options={}, **kwargs):

        header['Content-Type'] = 'application/json'

        if isinstance(json_dict, dict):
            post_data = json.dumps(json_dict, **json_options)
        else:
            post_data = json_dict

        log.debug('post_data=%s', post_data)

        content, code, header = self.request('post', url, header, post_data, **kwargs)

        return content

    @timeit
    def post_xml(self, url, xml_dict={}, header={}, **kwargs):

        header['Content-Type'] = 'application/xml'

        if isinstance(xml_dict, dict):
            post_data = dict2xml(xml_dict)
        else:
            post_data = xml_dict

        log.debug('post_data=%s', post_data)

        content, code, header = self.request('post', url, header, post_data, **kwargs)

        return content

    @timeit
    def post_file(self, url, data={}, files={}):
        raise NotImplementedError()

    def request(self, method, url, header, post_data=None):
        raise NotImplementedError()

class Urllib3Client(HTTPClient):
    name = 'urllib3'

    def request(self, method, url, header, post_data=None,  **kwargs):
        import urllib3
        import certifi
        urllib3.disable_warnings()

        pool_kwargs = {}
        if self._verify_ssl_certs:
            pool_kwargs['cert_reqs'] = 'CERT_REQUIRED'
            pool_kwargs['ca_certs'] = certifi.where()

        # 如果是长连接模式
        if self._conn_pool:
            global conn_pool
            if not conn_pool:
                conn_pool = urllib3.PoolManager(num_pools=max(100, self._conn_pool),
                                                maxsize=max(100, self._conn_pool),
                                                **pool_kwargs)
            conn = conn_pool
        else:
            conn = urllib3.PoolManager(**pool_kwargs)

        resp = conn.request(method=method, url=url,
                              body=post_data, headers=header,
                              timeout=self._timeout,
                              redirect=self._allow_redirect,
                              **kwargs)

        self.content, self.code, self.header = resp.data, resp.status, resp.headers

        return self.content, self.code, self.header

class RequestsClient(HTTPClient):
    name = 'requests'

    @timeit
    def post_file(self, url, data={}, files={}, header={}, **kwargs):
        '''
        requests发文件方便一些  就不实现协议报文了
        '''

        content, code, header = self.request('post', url, header, post_data=data, files=files, **kwargs)

        return content

    def request(self, method, url, header, post_data=None, files={}, **kwargs):

        # 如果是长连接模式
        if self._conn_pool:
            global conn_pool
            if not conn_pool:
                import requests
                conn_pool = requests.Session()
            requests = conn_pool
        else:
            import requests

        resp = requests.request(method,
                                  url,
                                  headers=header,
                                  data=post_data,
                                  timeout=self._timeout,
                                  files=files,
                                  verify=self._verify_ssl_certs,
                                  allow_redirects=self._allow_redirect,
                                  **kwargs)

        self.content, self.code, self.header = resp.content, resp.status_code, resp.headers

        return self.content, self.code, self.header

class PycurlClient(HTTPClient):
    name = 'pycurl'

    def _curl_debug_log(self, debug_type, debug_msg):
        '''
        from tornado
        '''
        debug_types = ('I', '<', '>', '<', '>')
        debug_msg = debug_msg.decode(self._charset)
        if debug_type == 0:
            log.debug('%s', debug_msg.strip())
        elif debug_type in (1, 2):
            for line in debug_msg.splitlines():
                log.debug('%s %s', debug_types[debug_type], line)
        elif debug_type == 4:
            log.debug('%s %r', debug_types[debug_type], debug_msg)

    def parse_header(self, data):
        header = {}
        for line in data.decode(self._charset).split('\r\n')[1:]:
            line = line.strip()
            if line:
                k,v  = line.split(':', 1)
                header[k.strip().title()] = v.strip()
        return header


    def request(self, method, url, header, post_data=None):
        import pycurl

        s = io.BytesIO()
        rheader = io.BytesIO()
        curl = pycurl.Curl()

        # 详细log
        curl.setopt(pycurl.VERBOSE, 1)
        curl.setopt(pycurl.DEBUGFUNCTION, self._curl_debug_log)

        if method == 'get':
            curl.setopt(pycurl.HTTPGET, 1)
        elif method == 'post':
            curl.setopt(pycurl.POST, 1)
            curl.setopt(pycurl.POSTFIELDS, post_data)
        else:
            curl.setopt(pycurl.CUSTOMREQUEST, method.upper())

        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.WRITEFUNCTION, s.write)
        curl.setopt(pycurl.HEADERFUNCTION, rheader.write)
        curl.setopt(pycurl.NOSIGNAL, 1)
        curl.setopt(pycurl.CONNECTTIMEOUT, 30)
        curl.setopt(pycurl.TIMEOUT, self._timeout)
        curl.setopt(pycurl.HTTPHEADER, ['%s: %s' % (k, v)
                    for k, v in header.items()])
        if self._verify_ssl_certs:
            curl.setopt(pycurl.CAINFO, os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data/ca-certificates.crt'))
        else:
            curl.setopt(pycurl.SSL_VERIFYHOST, False)

        curl.perform()

        # 记录时间啦
        http_code = curl.getinfo(pycurl.HTTP_CODE)
        http_conn_time =  curl.getinfo(pycurl.CONNECT_TIME)
        http_dns_time = curl.getinfo(pycurl.NAMELOOKUP_TIME)
        http_pre_tran =  curl.getinfo(pycurl.PRETRANSFER_TIME)
        http_start_tran =  curl.getinfo(pycurl.STARTTRANSFER_TIME)
        http_total_time = curl.getinfo(pycurl.TOTAL_TIME)
        http_size = curl.getinfo(pycurl.SIZE_DOWNLOAD)
        log.info('server=HTTPClient|func=pycurl_time|http_code=%d|http_size=%d|dns_time=%d|conn_time=%d|pre_tran=%d|start_tran=%d|total_time=%d'%(
                http_code,http_size,int(http_dns_time*1000000),int(http_conn_time*1000000),
                int(http_pre_tran*1000000),int(http_start_tran*1000000),int(http_total_time*1000000)))

        rbody = s.getvalue()
        rcode = curl.getinfo(pycurl.RESPONSE_CODE)

        self.content, self.code, self.header = rbody, rcode, self.parse_header(rheader.getvalue())
        return self.content, self.code, self.header

class UrllibClient(HTTPClient):
    name = 'urllib'

    def request(self, method, url, header, post_data=None, handlers=[]):

        if isinstance(post_data, str):
            post_data = post_data.encode(self._charset)
        req = urllib.request.Request(url, post_data, header)

        if method not in ('get', 'post'):
            req.get_method = lambda: method.upper()
        try:
            if not self._verify_ssl_certs and hasattr(ssl, 'SSLContext'):
                # 这里 verify_ssl_certs=False 仅用于调试
                response = urllib.request.urlopen(req, timeout=self._timeout, context=ssl._create_unverified_context())
            elif handlers:
                opener = urllib.request.build_opener(*handlers)
                response = opener.open(req,timeout=self._timeout)
            else:
                response = urllib.request.urlopen(req, timeout=self._timeout)

            rbody = response.read()
            rcode = response.code
            header = dict((k.title(),v) for k,v in dict(response.info()).items())
        except urllib.request.HTTPError as e:
            rbody = e.read()
            rcode = e.code
            header = dict((k.title(),v) for k,v in dict(e.info()).items())

        self.content, self.code, self.header = rbody, rcode, header

        return self.content, self.code, self.header

# 为了兼容
Urllib2Client = UrllibClient

#---------------------------
#   一些工具函数
#---------------------------

class HTTPSClientAuthHandler (urllib.request.HTTPSHandler):
    '''
    https 双向验证handler  用于urllib
    Urllib2Client().post('https://api.mch.weixin.qq.com/secapi/pay/refund', handlers=[HTTPSClientAuthHandler('apiclient_key.pem', 'apiclient_cert.pem')])
    '''
    def __init__(self, key, cert):
        urllib.request.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return http.client.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

    def __str__(self):
        return '<HTTPSClientAuthHandler key=%s cert=%s>' % ( self.key, self.cert )
    __repr__ = __str__



#----------TEST------------------

def test_get():
    for i in [PycurlClient,RequestsClient, Urllib3Client, Urllib2Client]:
        i().get('http://httpbin.org/get')

def test_post():
    for i in [PycurlClient,RequestsClient, Urllib3Client, Urllib2Client]:
        ret = i().post('http://httpbin.org/post', {'a':'b'})
        ret = json.loads(ret)
        assert ret['form']['a'] == 'b'

def test_post_json():
    for i in [PycurlClient,RequestsClient, Urllib3Client, Urllib2Client]:
        ret = i().post_json('http://httpbin.org/post', {'a':'b'})
        ret = json.loads(ret)
        assert ret['json']['a'] == 'b'

def test_post_xml():
    for i in [PycurlClient,RequestsClient, Urllib3Client, Urllib2Client]:
        ret = i().post_xml('http://httpbin.org/post', {'a':'b'})

def test_install():
    c = install('urllib2')
    c.get('http://baidu.com')
    c.get('http://baidu.com')
    c.get('http://baidu.com')

def test_long_conn():
    for i in range(5):
        RequestsClient(allow_redirect=True,conn_pool = True,verify_ssl_certs = True).get('https://httpbin.org/headers')
    global conn_pool
    conn_pool = None
    for i in range(5):
        Urllib3Client(allow_redirect=True,conn_pool = True,verify_ssl_certs = True).get('https://httpbin.org/headers')

def test_header():
    for client in [PycurlClient, RequestsClient, Urllib2Client, Urllib3Client]:
        c = client()
        c.post('http://httpbin.org/post',header={'X-testtest': 'test'})
        print(c.header)

def test_binary():
    Urllib2Client().get('https://www.baidu.com/img/bd_logo1.png')
    Urllib2Client().get('http://baidu.com')

def test_post_file():
    RequestsClient().post_file('http://httpbin.org/post', {'key1':'value1'}, files={'file1': open('__init__.py', 'rb')})

def test_urllib3():
    Urllib3Client().post_xml('http://httpbin.org/post',{'a':'1'})
    Urllib3Client(conn_pool=True).post_xml('http://httpbin.org/post',{'a':'1'})

if __name__ == '__main__':
    import logger
    logger.install('stdout')

    test_get()
    #  test_post()
    #  test_post_json()
    #  test_post_xml()
    #  test_install()
    #  test_long_conn()
    #  test_header()
    #  test_binary()
    #  test_post_file()
    #  test_urllib3()

