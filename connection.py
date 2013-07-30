import logging
import functools

from gevent import socket

log = logging.getLogger('connection')

class Closed(Exception):
    pass

class Stream:
    def __init__(self, addr, timeout=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if timeout is not None:
            sock.settimeout(timeout)
        log.info('connecting to peer at {}'.format(addr))
        try:
            sock.connect(addr)        
        except (socket.timeout, socket.error) as e:
            raise Closed(e)
        self.sock = sock

    def recv(self, n):        
        try:
            data = _recvall(self.sock, n)
        except (socket.timeout, socket.error) as e:
            raise Closed(e)

        if data is None: # peer socket is closed
            raise Closed('peer closed connection')

        return data 

    def send(self, data):
        try:
            _sendall(self.sock, data)
        except (socket.timeout, socket.error) as e:
            raise Closed(e)

    def close(self):
        self.sock.close()

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

def _sendall(sock, data):
    sock.sendall(data)
