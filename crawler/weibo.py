#!/usr/bin/env python
# -*- coding: utf-8 -*-
__version__ = '1.04'
__author__ = 'Liao Xuefeng (askxuefeng@gmail.com)'

'''
Python client SDK for sina weibo API using OAuth 2.
'''

try:
    import json
except ImportError:
    import simplejson as json
import time
import urllib
import urllib2
import logging
import pickle
import time
import logging

log = logging.getLogger(__name__)

class APIError(StandardError):
    ''' raise APIError if got failed json message. '''
    def __init__(self, error_code, error, request):
        self.error_code = error_code
        self.error = error
        self.request = request
        StandardError.__init__(self, error)
    def __str__(self):
        return 'APIError: %s: %s, request: %s' % (self.error_code, self.error, self.request)

def _encode_params(**kw):
    args = []
    for k, v in kw.iteritems():
        qv = v.encode('utf-8') if isinstance(v, unicode) else str(v)
        args.append('%s=%s' % (k, urllib.quote(qv)))
    return '&'.join(args)

def _encode_multipart(**kw):
    ''' Build a multipart/form-data body with generated random boundary. '''
    boundary = '----------%s' % hex(int(time.time() * 1000))
    data = []
    for k, v in kw.iteritems():
        data.append('--%s' % boundary)
        if hasattr(v, 'read'):
            # file-like object:
            ext = ''
            filename = getattr(v, 'name', '')
            n = filename.rfind('.')
            if n != (-1):
                ext = filename[n:].lower()
            content = v.read()
            data.append('Content-Disposition: form-data; name="%s"; filename="hidden"' % k)
            data.append('Content-Length: %d' % len(content))
            data.append('Content-Type: %s\r\n' % _guess_content_type(ext))
            data.append(content)
        else:
            data.append('Content-Disposition: form-data; name="%s"\r\n' % k)
            data.append(v.encode('utf-8') if isinstance(v, unicode) else v)
    data.append('--%s--\r\n' % boundary)
    return '\r\n'.join(data), boundary

_CONTENT_TYPES = { '.png': 'image/png', '.gif': 'image/gif', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.jpe': 'image/jpeg' }

def _guess_content_type(ext):
    return _CONTENT_TYPES.get(ext, 'application/octet-stream')

_HTTP_GET = 0
_HTTP_POST = 1
_HTTP_UPLOAD = 2

def _http_get(url, authorization=None, **kw):
    logging.info('GET %s' % url)
    return _http_call(url, _HTTP_GET, authorization, **kw)

def _http_post(url, authorization=None, **kw):
    logging.info('POST %s' % url)
    return _http_call(url, _HTTP_POST, authorization, **kw)

def _http_upload(url, authorization=None, **kw):
    logging.info('MULTIPART POST %s' % url)
    return _http_call(url, _HTTP_UPLOAD, authorization, **kw)

def _http_call(the_url, method, authorization, **kw):
    ''' send an http request and expect to return a json object if no error. '''
    params = None
    boundary = None
    if method==_HTTP_UPLOAD:
        params, boundary = _encode_multipart(**kw)
    else:
        params = _encode_params(**kw)
    http_url = '%s?%s' % (the_url, params) if method==_HTTP_GET else the_url
    http_body = None if method==_HTTP_GET else params
    try:
        log.debug("calling sinaAPI:%s %s", http_url, http_body)
        req = urllib2.Request(http_url, data=http_body)
        if authorization:
            req.add_header('Authorization', 'OAuth2 %s' % authorization)
        if boundary:
            req.add_header('Content-Type', 'multipart/form-data; boundary=%s' % boundary)
        resp = urllib2.urlopen(req)
        respStr = resp.read()
        log.debug("sinaAPI return:%s %s", resp.getcode(), respStr[:10])
        return respStr
    except urllib2.HTTPError,e:
        body = e.read()
        r = json.loads(body)
        log.error("HTTPError(%d): %s", e.code, body)
        raise APIError(r.get('error_code',""), r.get('error', ''), r.get('request', ''))

class HttpObject(object):
    def __init__(self, client, method):
        self.client = client
        self.method = method
    def __getattr__(self, attr):
        def wrap(**kw):
#            if self.client.is_expires():
#                raise APIError('21327', 'expired_token', attr)
            return _http_call('%s%s.json' % (self.client.api_url, attr.replace('__', '/')), self.method, self.client.access_token, **kw)
        return wrap

class APIClient(object):
    ''' API client using synchronized invocation. '''
    def __init__(self, access_token , domain='api.weibo.com', version='2'):
        self.auth_url = 'https://%s/oauth2/' % domain
        self.api_url = 'https://%s/%s/' % (domain, version)
        self.access_token = access_token
        self.get = HttpObject(self, _HTTP_GET)
        self.post = HttpObject(self, _HTTP_POST)
        self.upload = HttpObject(self, _HTTP_UPLOAD)        

    def __getattr__(self, attr):
        return getattr(self.get, attr)
