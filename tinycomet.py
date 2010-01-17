# -*- coding:utf-8 -*-

from eventlet import wsgi
from eventlet.green import socket, threading
from eventlet.corolocal import local
import urllib
import wsgiref
import functools

ERRORS = {
    400: 'Bad Request',
    404: 'Not Found',
    408: 'Request Timeout'
}
WAIT_INTERVAL = 2.5
_comet_storage = {}
_update_lock = threading.Condition()

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
    def __init__(self):
        self.payload = ''
        self.content_type = 'application/octet-stream'
        self.last_update = 0
        self.finished = False

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
    try:
        data = _comet_storage[uuid]
    except KeyError:
        return error_response(start_response, 404)
    query = parse_query(env)
    
    if 'since' in query:
        since = int(query['since'], 10)
        timeout = int(query.get('timeout', '10'), 10)

        # wait for update
        with _update_lock:
            while data.last_update <= since:
                wait = WAIT_INTERVAL if timeout > WAIT_INTERVAL else timeout
                _update_lock.wait(wait)
                timeout -= wait
                if timeout < 0.01:
                    # return timeout
                    return error_response(start_response, 408)
    
    if data.finished:
        # remove from storage
        del _comet_storage[uuid]
                
    headers = [
        ('Content-Type', data.content_type),
        ('X-TC-Timestamp', '%d' % data.last_update)
    ]
    start_response('200 OK', headers)
    return [data.payload]

@make_comet_receiver
def update_receiver(uuid, env, start_response):
    if env['REQUEST_METHOD'] != 'POST':
        return error_response(start_response, 400)
    query = parse_query(env)
    
    data = _comet_storage.setdefault(uuid, CometData())
    data.payload = env['wsgi.input'].read()
    data.last_update = _logical_timer.get_local()
    data.content_type = query.get('content_type', 'application/octet-stream')
    
    if query.get('finished', '') == '1':
        # finished
        data.finished = True
    
    with _update_lock:
        _update_lock.notify_all()

    headers = [
#        ('Content-Type', data.content_type),
        ('X-TC-Timestamp', '%d' % data.last_update)
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