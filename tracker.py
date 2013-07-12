import random
import struct
import cStringIO
import binascii
import logging

import hashlib
import metainfo
import bencode

import construct as c
from gevent import socket

class Error(Exception):
    pass

MAX_PACKET_SIZE = 2**16

log = logging.getLogger('tracker')

class udp:
    def __init__(self, addr, timeout=None):
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.conn.settimeout(timeout)
        self.conn.connect(addr)
        log.debug('connected to {}'.format(self.conn.getpeername()))
        self.addr = addr
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

        log.debug('transaction: {}'.format(tx))
        self.conn.send(msg)

        # handle response
        msg = self.conn.recv(MAX_PACKET_SIZE)

        resp = c.Struct('response',
                c.UBInt32('action'),
                c.UBInt32('transaction_id'),
                c.UBInt64('connection_id'),
            )

        obj = resp.parse(msg)
        log.debug('connection: {}'.format(obj.connection_id))

        if obj.action != 0:
            raise Error('Incorrect action: {}'.format(obj))

        if obj.transaction_id != tx:
            raise Error('Unexpected transaction_id: {}'.format(obj))

        self.id = obj.connection_id

    def announce(self, peer, data, **kw):
        
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
            'event': kw.get('event', 0),
            'ip_addr': 0,
            'key': 0,
            'num_want': kw.get('num_want', -1)
        }
        kw.update(peer)
        kw.update(data)
        obj = c.Container(**kw)

        msg = req.build(obj)
        log.debug('request peers for {}'.format(binascii.hexlify(obj.info_hash)))
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
        log.debug('got {} peers'.format(len(peer_list)))
        
        fields = ('interval', 'seeders', 'leechers')
        log.info('statistics: {}'.format({k: obj[k] for k in fields}))
        return peer_list

def parse_address(url):
    ''' Parse tracker address of the form "udp://address:port/".
    '''
    proto, url = url.split('://')
    assert proto == 'udp'

    addr, url = url.split(':')
    port, end = url.split('/')

    assert end == ''
    port = int(port)

    return (addr, port)

def get_peers(meta, peer_id, port):

    peer = dict(peer_id=peer_id, port=port)
    data = dict(info_hash=meta.info_hash, uploaded=0, downloaded=0, left=meta.length)

    while True:
        try:            
            log.debug('connecting to {}'.format(meta.announce_addr))
            addr = parse_address(meta.announce_addr)
            tracker = udp(addr, timeout=10)
            tracker.connect()
            return tracker.announce(peer, data, num_want=10)
        except socket.timeout:
            log.warning('timeout')
        except socket.error, e:
            log.warning('unreachable: {}'.format(tracker.conn.getpeername()))

def test_tracker(fname):
    logging.basicConfig(
        format='%(asctime)-15s [%(levelname)s] %(message)s', 
        level=logging.DEBUG)
    meta = metainfo.MetaInfo(fname)
    peers = get_peers(meta, peer_id=metainfo.hash('test_tracker'), port=6889)
    print peers

if __name__ == '__main__':
    import sys
    fname, = sys.argv[1:]
    test_tracker(fname)
