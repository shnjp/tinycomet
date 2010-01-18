# -*- coding:utf-8 -*-

from eventlet import wsgi
from eventlet.green import socket, threading
from eventlet.corolocal import local
import urllib
import wsgiref
import functools
import time
import json
import binascii

ERRORS = {
    400: 'Bad Request',
    404: 'Not Found',
    408: 'Request Timeout'
}
WAIT_INTERVAL = 2.5
_comet_storage = {}
_update_lock = threading.Condition()

class TimeoutException(Exception):
    pass

class LogicalTimer(object):
    def __init__(self):
        self._time = 1
    
    def get_global(self):
        return self._time
    
    def get_local(self):
        return local.logical_time
    
    def inc(self):
        self._time += 1
        return self._time
    
    def timer_middleware(self, app):
        def middleware(env, start_response):
            local.logical_time = self.inc()
            return app(env, start_response)
        return middleware
_logical_timer = LogicalTimer()

class CometData(object):
    def __init__(self, payload, last_update, content_type='application/octet-stream', finished=False):
        self.payload = payload
        self.last_update = last_update
        self.content_type = content_type
        self.finished = finished
    
    def __repr__(self):
        return '<CometData (%d):`%r`:%s>' % (self.last_update, self.payload, self.content_type)
    
def make_dispatch_middleware(urlmap):
    def app(env, start_response):
        path = env['PATH_INFO']
        
        for k, v in urlmap.iteritems():
            if path.startswith(k):
                env['SHIFT_PATH_INFO'] = path[len(k):]
                return v(env, start_response)
        
        return error_response(start_response, 404)
    return app

def error_response(start_response, code):
    msg = ERRORS.get(code, '%d' % code)
    start_response(
        '%d %s' % (code, msg),
        [('Content-Type', 'text/plain')]
    )
    return [msg, '\r\n']

def parse_query(env):
    query = env['QUERY_STRING']
    if not query:
        return {}
    query = [x.split('=', 1) for x in query.split('&')]
    query = dict((x[0], urllib.unquote(x[1])) for x in query)
    return query    

def make_comet_receiver(func):
    @functools.wraps(func)
    def receiver(env, start_response):
        path = env['SHIFT_PATH_INFO']
        if not path:
            return error_response(start_response, 404)
        
        return func(path, env, start_response)
    return receiver

@make_comet_receiver
def wait_receiver(uuid, env, start_response):
    query = parse_query(env)
    
    since = int(query['since'], 10) if 'since' in query else None
    if since:
        timeout = float(query['timeout']) if 'timeout' in query else None
        last = time.time() + timeout if timeout else None
    
    try:
        # もっと綺麗にかけないかしら
        with _update_lock:
            while True:
                data = _comet_storage[uuid]
                if not since or data.last_update > since:
                    break
                
                # wait for update
                _update_lock.wait(timeout)
                timeout = last - time.time() if last else None
                if timeout is not None and timeout < 0:
                    raise TimeoutException
    except KeyError:
        return error_response(start_response, 404)
    except TimeoutException:
        return error_response(start_response, 408)

    if data.finished:
        # remove from storage
        del _comet_storage[uuid]

    jsonp = query.get('callback')
    
    headers = [
        ('X-TC-Timestamp', '%d' % data.last_update)
    ]
    
    if jsonp is None:
        headers.append(('Content-Type', data.content_type))
        if data.finished:
            headers.append(('X-TC-Removed', 'removed'))

        start_response('200 OK', headers)
        return [data.payload]
    else:
        obj = {
            'last_update': data.last_update,
            'content_type': data.content_type,
            'finished': data.finished
        }
        if data.content_type in ['application/json']:
            obj['payload'] = json.loads(data.payload)
        elif data.content_type.startswith('text/'):
            obj['payload_text'] = data.payload
        else:
            obj['payload_base64'] = binascii.b2a_base64(data.payload)
        headers.append(('Content-Type', 'text/javascript'))
        print json.dumps(obj)
        start_response('200 OK', headers)
        return [jsonp, '(', json.dumps(obj), ')']
        

@make_comet_receiver
def update_receiver(uuid, env, start_response):
    if env['REQUEST_METHOD'] != 'POST':
        return error_response(start_response, 400)
    query = parse_query(env)
    
    last_update = _logical_timer.get_local()
    with _update_lock:
        _comet_storage[uuid] = CometData(
            env['wsgi.input'].read(),
            last_update,
            content_type=query.get('content_type', 'application/octet-stream'),
            finished=query.get('finished', '') == '1'
        )
        _update_lock.notify_all()

    headers = [
#        ('Content-Type', data.content_type),
        ('X-TC-Timestamp', '%d' % last_update)
    ]
    
    start_response('201 Created', headers)
    return []

def get_options(args):
    from optparse import OptionParser
    
    parser = OptionParser()
    parser.add_option("-b", "--bind", dest="bind",
                      help="bind address",
                      default='127.0.0.1:8090')

    options, args = parser.parse_args(args)
    return options
    
def main(args=''):
    options = get_options(args)
        
    # bind and listen
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    bind = options.bind.split(':')
    if len(bind) == 1:
        bind = t[0], '8090'
    sock.bind((bind[0], int(bind[1], 10)))
    sock.listen(500)

    # launch wsgi app
    app = make_dispatch_middleware({
        '/wait/': wait_receiver,
        '/update/': update_receiver,
#        '/wait_multi': wait_multi_receiver
    })
    app = _logical_timer.timer_middleware(app)
    wsgi.server(sock, app)

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])