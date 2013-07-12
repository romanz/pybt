import logging

from gevent import socket

log = logging.getLogger('connection')

class Connection:
    def __init__(self, addr, timeout=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if timeout is not None:
            sock.settimeout(timeout)
        log.info('connecting to peer at {}'.format(addr))
        sock.connect(addr)        
        self.sock = sock

    def recv(self, n):        
        data = ''
        while True:
            left = n - len(data)
            if left == 0:
                return data
            buf = self.sock.recv(left)
            data = data + buf

    def send(self, data):
        self.sock.sendall(data)
