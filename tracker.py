import re
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

INIT_ID = 0x41727101980

connect_req = c.Struct('request', 
    c.UBInt64('connection_id'),
    c.UBInt32('action'),
    c.UBInt32('transaction_id')
)

connect_resp = c.Struct('response',
    c.UBInt32('action'),
    c.UBInt32('transaction_id'),
    c.UBInt64('connection_id'),
)

announce_req = c.Struct('request',
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

announce_resp = c.Struct('response', 
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

scrape_req = c.Struct('request', 
    c.UBInt64('connection_id'),
    c.UBInt32('action'),
    c.UBInt32('transaction_id'),
    c.GreedyRange(
        c.Struct('hashes',
            c.Bytes('info_hash', 20),
        )
    )
)

scrape_resp = c.Struct('response',
    c.UBInt32('action'),
    c.UBInt32('transaction_id'),
    c.GreedyRange(
        c.Struct('stats',
            c.UBInt32('seeders'),
            c.UBInt32('completed'),
            c.UBInt32('leechers'),
        )
    )
)

class udp:
    def __init__(self, addr, timeout=None):
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.conn.settimeout(timeout)
        self.conn.connect(addr)
        log.debug('connected to {}'.format(self.conn.getpeername()))
        self.addr = addr
        self.id = None

    def connect(self):
        tx = random.getrandbits(32)
        obj = c.Container(connection_id=INIT_ID, action=0, transaction_id=tx)
        msg = connect_req.build(obj)

        log.debug('transaction: {}'.format(tx))
        self.conn.send(msg)

        # handle response
        msg = self.conn.recv(MAX_PACKET_SIZE)

        obj = connect_resp.parse(msg)
        log.debug('connection: {}'.format(obj.connection_id))

        if obj.action != 0:
            raise Error('Incorrect action: {}'.format(obj))

        if obj.transaction_id != tx:
            raise Error('Unexpected transaction_id: {}'.format(obj))

        self.id = obj.connection_id

    def announce(self, peer, data, **kw):
        
        if self.conn is None:
            raise Error('Must connect to tracker before announcement')

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

        log.debug('request peers for {}'.format(binascii.hexlify(obj.info_hash)))
        msg = announce_req.build(obj)
        self.conn.send(msg)

        # handle response
        msg = self.conn.recv(MAX_PACKET_SIZE)        
        obj = announce_resp.parse(msg)

        if obj.action != 1:
            raise Error('Incorrect action: {}'.format(obj))

        if obj.transaction_id != tx:
            raise Error('Unexpected transaction_id: {}'.format(obj))

        peer_list = [('{}.{}.{}.{}'.format(*p.addr), p.port) for p in obj.peer]
        log.info('got {} peers from {}'.format(len(peer_list), self.conn.getpeername()))
        
        fields = ('interval', 'seeders', 'leechers')
        log.info('statistics: {}'.format({k: obj[k] for k in fields}))
        return peer_list

def parse_address(url):
    ''' Parse tracker address of the form "udp://address:port/".
    '''
    url = url.rstrip('/')
    proto, url = url.split('://')
    assert proto == 'udp'

    addr, port = url.split(':')
    port = int(port)

    return (addr, port)

def get_peers(addr, info_hash, peer_id, timeout=None, port=6889, num_want=-1, 
                uploaded=0, downloaded=0, left=0):

    peer = dict(peer_id=peer_id, port=port)
    data = dict(info_hash=info_hash, uploaded=uploaded, downloaded=downloaded, left=left)

    addr = parse_address(addr)
    while True:
        try:            
            log.debug('connecting to {}'.format(addr))
            tracker = udp(addr, timeout=timeout)
            tracker.connect()
            return tracker.announce(peer, data, num_want=num_want)
        except socket.timeout:
            log.warning('timeout')
        except socket.error, e:
            log.warning('unreachable: {}'.format(addr))

def test_tracker(arg):
    logging.basicConfig(
        format='%(asctime)-15s [%(levelname)s] %(message)s', 
        level=logging.DEBUG)
    
    meta = metainfo.MetaInfo(arg)
    peers = get_peers(meta, peer_id=metainfo.hash('test_tracker'), port=6889, timeout=10)
    print peers

if __name__ == '__main__':
    import sys
    arg, = sys.argv[1:]
    test_tracker(arg)
