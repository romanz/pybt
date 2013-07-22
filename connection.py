import logging
import functools

import gevent
from gevent import socket

log = logging.getLogger('connection')

class Closed(Exception):
    pass

class Connection:
    def __init__(self, addr, timeout=None, rx=None, tx=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if timeout is not None:
            sock.settimeout(timeout)
        log.info('connecting to peer at {}'.format(addr))
        try:
            sock.connect(addr)        
        except (socket.timeout, socket.error) as e:
            raise Closed(e)
        self.sock = sock
        self.rx = _create_throttle(rx)
        self.tx = _create_throttle(tx)

    def recv(self, n):        
        try:
            data = _recvall(self.sock, n)
        except (socket.timeout, socket.error) as e:
            raise Closed(e)

        if data is None: # peer socket is closed
            raise Closed('peer closed connection')

        return self.rx(data) # throttle RX bandwidth

    def send(self, data):
        try:
            data = self.tx(data) # throttle TX bandwidth
            self.sock.sendall(data)
        except (socket.timeout, socket.error) as e:
            raise Closed(e)

def _recvall(sock, n):
        data = ''
        while True:
            left = n - len(data)
            if left == 0:
                return data

            buf = sock.recv(left)
            if not buf: # peer socket is closed
                return None

            data = data + buf

def _create_throttle(obj):

    if obj is None:
        return (lambda x: x) # no-op function

    if isinstance(obj, (int, float, long)):
        bw = float(obj)
        return functools.partial(throttle, bandwidth=bw)

    return obj # return object as is

def throttle(data, bandwidth=None):
     # time needed for data to be sent/received
    dt = (len(data) / bandwidth) if bandwidth else 0.0
    gevent.sleep(dt)
    return data
