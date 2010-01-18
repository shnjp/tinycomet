# -*- coding:utf-8 -*-

import threading
import tinycomet
import urllib
import time

COMET_ROOT = 'http://localhost:8090/'

def update_after_wait(uuid, data, query='', wait=1.0):
    time.sleep(wait)

    selector = COMET_ROOT + 'update/%s' % uuid
    if query:
        selector += '?' + query
    res = urllib.urlopen(selector, data)
    assert res.code == 201

def test_update():
    # can create new data and fetch it
    TEST_DATA = 'mogemoge'

    res = urllib.urlopen(COMET_ROOT + 'wait/test')
    assert res.code == 404

    res = urllib.urlopen(COMET_ROOT + 'update/test', TEST_DATA)
    assert res.code == 201

    res = urllib.urlopen(COMET_ROOT + 'wait/test')
    assert res.code == 200
    assert res.read() == TEST_DATA

def test_wait():
    # can wait for the data finished
    res = urllib.urlopen(COMET_ROOT + 'update/test', '1')
    assert res.code == 201
    timestamp = res.info()['X-TC-Timestamp']
    
    threading.Thread(target=update_after_wait, args=('test', '2', 'finished=1')).start()

    res = urllib.urlopen(COMET_ROOT + 'wait/test?since=%s' % timestamp)
    assert res.code == 200
    assert res.read() == '2'

    res = urllib.urlopen(COMET_ROOT + 'wait/test')
    assert res.code == 404

def test_timeout():
    # can wait for the data finished
    res = urllib.urlopen(COMET_ROOT + 'update/timeout_test', 'foobar')
    assert res.code == 201
    timestamp = res.info()['X-TC-Timestamp']
    
    res = urllib.urlopen(COMET_ROOT + 'wait/timeout_test?since=%s&timeout=2' % timestamp)
    assert res.code == 408
        
def test_main():
    # launch comet server
    th = threading.Thread(target=tinycomet.main, args=([],))
    th.daemon = True
    th.start()
    
    print 'test update'
    test_update()
    print 'test wait'
    test_wait()
    print 'test timeout'
    test_timeout()
    
test_main()