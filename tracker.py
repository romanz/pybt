import random
import struct
import cStringIO
import binascii

import hashlib
import metainfo
import bencode

import construct as c
from gevent import socket

class Error(Exception):
    pass

MAX_PACKET_SIZE = 1024

class udp:
    def __init__(self, addr):
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.conn.connect(addr)
        self.id = None

    def connect(self):
        req = c.Struct('request', 
                c.UBInt64('connection_id'),
                c.UBInt32('action'),
                c.UBInt32('transaction_id')
            )
        tx = random.getrandbits(32)
        obj = c.Container(connection_id=0x41727101980, action=0, transaction_id=tx)
        msg = req.build(obj)
        self.conn.send(msg)

        # handle response
        resp = c.Struct('response',
                c.UBInt32('action'),
                c.UBInt32('transaction_id'),
                c.UBInt64('connection_id'),
            )
        msg = self.conn.recv(MAX_PACKET_SIZE)
        obj = resp.parse(msg)

        if obj.action != 0:
            raise Error('Incorrect action: {}'.format(obj))

        if obj.transaction_id != tx:
            raise Error('Unexpected transaction_id: {}'.format(obj))

        self.id = obj.connection_id

    def announce(self, peer, status, event=0):
        
        if self.conn is None:
            raise Error('Must connect to tracker before announcement')

        req = c.Struct('request',
                c.UBInt64('connection_id'),
                c.UBInt32('action'),
                c.UBInt32('transaction_id'),
                c.Bytes('info_hash', 20),
                c.Bytes('peer_id', 20),
                c.UBInt64('downloaded'),
                c.UBInt64('left'),
                c.UBInt64('uploaded'),
                c.UBInt32('event'),
                c.UBInt32('ip_addr'),
                c.UBInt32('key'),
                c.SBInt32('num_want'),
                c.UBInt16('port'),
            )

        tx = random.getrandbits(32)
        kw = {
            'transaction_id': tx, 
            'action': 1, 
            'connection_id': self.id, 
            'event': event,
            'ip_addr': 0,
            'key': 0,
            'num_want': -1}
        kw.update(peer)
        kw.update(status)
        obj = c.Container(**kw)

        msg = req.build(obj)
        self.conn.send(msg)

        # handle response
        msg = self.conn.recv(MAX_PACKET_SIZE)
        
        if msg[0] != '\x00':
            raise Error('Invalid response: {}'.format(repr(msg)))

        resp = c.Struct('response', 
            c.UBInt32('action'), 
            c.UBInt32('transaction_id'),
            c.UBInt32('interval'),
            c.UBInt32('leechers'),
            c.UBInt32('seeders'),
            c.GreedyRange(
                c.Struct('peer',
                    c.Array(4, c.UBInt8('addr')),
                    c.UBInt16('port')
                )
            )
        )

        obj = resp.parse(msg)

        if obj.action != 1:
            raise Error('Incorrect action: {}'.format(obj))

        if obj.transaction_id != tx:
            raise Error('Unexpected transaction_id: {}'.format(obj))

        peer_list = [('{}.{}.{}.{}'.format(*p.addr), p.port) for p in obj.peer]
        
        fields = ('interval', 'seeders', 'leechers')
        return peer_list, {k: obj[k] for k in fields}

def get_peers(meta, peer_id, port):
    tracker = udp(meta.announce_addr)
    tracker.connect()
    peer = dict(peer_id=peer_id, port=port)
    status = dict(info_hash=meta.info_hash, uploaded=0, downloaded=0, left=meta.length)
    peers, stats = tracker.announce(peer, status)
    return peers, stats

def test_tracker(fname):
    meta = metainfo.MetaInfo(fname)
    peers, stats = get_peers(meta, peer_id=metainfo.hash('test'), port=6889)
    print stats
    print '-' * 80
    print peers

if __name__ == '__main__':
    import sys
    fname, = sys.argv[1:]
    test_tracker(fname)
