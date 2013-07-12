import logging

from gevent import socket

log = logging.getLogger('connection')

class Closed(Exception):
    pass

class Timeout(Exception):
    pass

class Connection:
    def __init__(self, addr, timeout=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if timeout is not None:
            sock.settimeout(timeout)
        log.info('connecting to peer at {}'.format(addr))
        try:
            sock.connect(addr)        
        except socket.timeout:
            raise Timeout()
        except socket.error:
            raise Closed()
        self.sock = sock

    def recv(self, n):        
        data = ''
        while True:
            left = n - len(data)
            if left == 0:
                return data

            try:
                buf = self.sock.recv(left)
            except socket.timeout:
                raise Timeout()
            except socket.error:
                raise Closed()

            if not buf: # peer socket is closed
                raise Closed()

            data = data + buf

    def send(self, data):
        self.sock.sendall(data)
