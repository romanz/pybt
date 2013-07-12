from gevent import socket

class Connection:
    def __init__(self, addr, timeout=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if timeout is not None:
            sock.settimeout(timeout)
        sock.connect(addr)
        self.sock = sock

    def recv(self, n):        
        data = ''
        while True:
            left = n - len(data)
            if left == 0:
                return data
            data = data + self.sock.recv(left)

    def send(self, data):
        self.sock.sendall(data)
