import logging

from gevent import socket

log = logging.getLogger('connection')

class Closed(Exception):
    pass

class Connection:
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
        data = ''
        while True:
            left = n - len(data)
            if left == 0:
                return data

            try:
                buf = self.sock.recv(left)
            except (socket.timeout, socket.error) as e:
                raise Closed(e)

            if not buf: # peer socket is closed
                raise Closed('peer closed connection')

            data = data + buf

    def send(self, data):
        try:
            self.sock.sendall(data)
        except (socket.timeout, socket.error) as e:
            raise Closed(e)
