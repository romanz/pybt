import hashlib
import logging
import itertools
import functools
import collections

import construct as c
import bitarray

import metainfo
import bencode
import tracker
import peer
import storage
import connection

log = logging.getLogger('Protocol')

class PeerProtocol:
    
    def __init__(self, host_id, data, conn):
        self.data = data
        self.conn = conn
        self.host_id = host_id
        self.peer_id = None

        self.peer_bits = self.data.bits.copy()
        self.peer_bits.setall(0) # nothing is yet known so assume peer has nothing

        self.state = {'am_choking': True, 'peer_interested': False,
                      'peer_choking': True, 'am_interested': False}

    def recv_cmd(self):
        lentype = c.UBInt32('length')
        L = self.conn.recv(lentype.sizeof())
        n = lentype.parse(L)
        data = self.conn.recv(n)
        msg = L + data
        return peer.parse_command(msg)

    def _receiver(self):
        while True:
            reply = self.recv_cmd()
            yield reply

    def send_cmd(self, name, **kw):
        msg = peer.build_command(name, **kw)
        self.conn.send(msg)
    
    def handle_reply(self, reply):

        if reply.name in {'unchoke', 'choke'}:
            self.state['am_choking'] = (reply.name == 'choke')
            log.info('host is {}d by peer'.format(reply.name))
            self.handle_event( choke=self.state['am_choking'] )
            return

        if reply.name == {'interested', 'uninterested'}:
            self.state['peer_interested'] = (reply.name == 'interested')
            log.info('peer is {}'.format(reply.name))
            return

        if reply.name in {'bitfield', 'have'}:
            if reply.name == 'bitfield':
                peer_bits = storage.bitfield(self.data.meta, reply.bits)
            else: # have
                peer_bits = self.peer_bits.copy()
                peer_bits[reply.index] = True

            log.info('peer has {} of {} pieces'.format(peer_bits.count(), 
                peer_bits.length()))

            self.peer_bits = peer_bits
            self.handle_event( peer_bits=peer_bits )

            needed = self.peer_bits & ~self.data.bits
            log.info('peer has {} needed pieces'.format(needed.count()))

            if any(needed) and self.state['am_interested'] == False:
                log.info('host is interested in peer')
                self.state['am_interested'] = True
                self.send_cmd('interested')
                return

            if not any(needed) and self.state['am_interested'] == True:
                log.info('host is uninterested in peer')
                self.state['am_interested'] = False
                self.send_cmd('uninterested')
                return

            return

        if reply.name == 'piece':
            log.debug('downloaded #{} @ {} [{}B]'.format(reply.index, reply.begin, len(reply.data)))
            self.data.write(index=reply.index, begin=reply.begin, data=reply.data)
            self.handle_event( piece=reply )
            return

        if reply.name == 'request':
            log.debug('peer request #{} @ {} [{}B]'.format(reply.index, reply.begin, reply.length))
            data = self.data.read(index=reply.index, begin=reply.begin, size=reply.length)

            self.send_cmd('piece', index=reply.index, begin=reply.begin, data=data)
            log.debug('peer upload  #{} @ {} [{}B]'.format(reply.index, reply.begin, len(data)))            
            return

        if reply.name == 'keep_alive':
            log.debug('got keep alive from peer')            
            return

        log.warning('unsupported reply: {!r}'.format(reply))

    def initialize(self):

        log.info('handshake to {!r}'.format(self.conn.sock.getpeername()))

        msg = peer.build_handshake(self.data.meta.info_hash, self.host_id)
        self.conn.send(msg)

        msg = self.conn.recv(len(msg)) # Assume reply message has the same length
        reply = peer.parse_handshake(msg)    
        assert reply.info_hash == self.data.meta.info_hash
        assert reply.pstr == 'BitTorrent protocol'
        self.peer_id = reply.peer_id

        downloader = Downloader(self.data)
        self.handle_event = functools.partial(downloader.handle_event, self)

        log.info('handshake from {!r}'.format(self.peer_id))

        log.info('updating peer with our bitfield')
        self.send_cmd('bitfield', bits=self.data.bits.tobytes())

        log.info('peer is unchoked by host')
        self.send_cmd('unchoke')

        self._pending_requests = set([])

    def loop(self):
        self.initialize()
        for reply in self._receiver():
            self.handle_reply(reply)
            if all(self.data.bits):
                return

Request = collections.namedtuple('Request', ['index', 'begin', 'length'])

def itake(iterable, n):
    return list(itertools.islice(iterable, n))

class Downloader:
    
    def __init__(self, data, block_size=2**14, queue_size=2**3):
        self.data = data
        self.block_size = block_size
        piece_indices = storage.indices(~data.bits) # indices to missing pieces
        # requests data structure: maps request to peers it was sent to

        reqs = ((req, set()) for req in self._generate(piece_indices))
        self.reqs = collections.OrderedDict(reqs)
        self.queue_size = queue_size

    def handle_event(self, peer, choke=None, peer_bits=None, piece=None):

        if choke is not None:
            # handle choke/unchoke message
            if choke:
                while peer._pending_requests:
                    req = peer._pending_requests.pop()
                    log.info('flush {} '.format(req))
                    self.reqs.remove(peer.peer_id)
            else: # unchoke
                self._request(peer)

        if peer_bits is not None: # handle bits/have message
            self._request(peer)

        if piece is not None: # handle piece message
            req = Request(piece.index, piece.begin, len(piece.data))
            peer._pending_requests.remove(req)
            peers = self.reqs.pop(req)
            assert peers == set([peer.peer_id]) # we expect only one request per block
            if all(r not in self.reqs for r in self._generate([req.index])):
                # all requests of this piece are recieved
                self.data.validate([req.index])

            self._request(peer)

    def _request(self, peer):

        if peer.state['am_choking']:
            return # this peer is choked

        while len(peer._pending_requests) < self.queue_size:
            bits = peer.peer_bits & ~self.data.bits
            if not any(bits): 
                return # this peer has no useful pieces

            # requests for blocks the peer has, and the host does not, and were not requsted yet
            reqs = (r for r, peers in self.reqs.items() if bits[r.index] and len(peers) == 0)
            reqs = itake(reqs, 1)
            if not reqs:
                return # no additional requests should be made

            req, = reqs
            log.debug('requesting #{} @ {} [{}B] '.format(req.index, req.begin, req.length))
            self.reqs[req].add( peer.peer_id )
            peer.send_cmd('request', **vars(req))
            peer._pending_requests.add(req)            

    def _generate(self, piece_indices):
        for index in piece_indices:
            piece_size = self.data.piece_size(index)
            offset = 0
            while offset < piece_size:
                size = min(self.block_size, piece_size - offset)
                yield Request(index, offset, size)
                offset = offset + size

