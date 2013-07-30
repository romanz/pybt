import os
import logging
import binascii
import itertools
from collections import OrderedDict
from collections import namedtuple

import peer
import bencode
import connection
import metainfo
import storage
import tracker

import gevent
import gevent.queue

log = logging.getLogger('download')

def metadata_request(conn, piece):
    obj = OrderedDict(msg_type=0, piece=piece)
    conn.send_cmd('extended', cmd=peer.UT_METADATA, msg=bencode.encode(obj))

def metadata_save(data):
    info_hash = metainfo.hash(data)
    h = binascii.hexlify(info_hash)
    log.info('saving metadata for {}'.format(h))
    with file(h + '.meta', 'wb') as f: 
        f.write(data)

    meta = metadata_load(info_hash)
    assert meta is not None # verify correct data is written to file
    return meta

def metadata_load(info_hash):
    h = binascii.hexlify(info_hash)
    fname = h + '.meta'
    if not os.path.exists(fname): 
        return None

    with file(fname, 'rb') as f: 
        data = f.read()
        
    if metainfo.hash(data) == info_hash:
        meta = metainfo.MetaInfo(bencode.decode(data))
        log.info('"{}" ({} hashes) = {:.1f}MB'.format(meta.name, 
            len(meta.hashes), meta.total / 1e6))
        return meta

def loop(dl, addr):
    ''' BitTorrent `Protocol main loop. 
    '''
    try:
        conn = connection.Stream(addr, timeout=60)
        conn = peer.Connection(conn) # peer message protocol wrapper

        conn.state['am_choking']        = True
        conn.state['peer_choking']      = True
        conn.state['am_interested']     = False
        conn.state['peer_interested']   = False
        
        # Handshake with peer        
        dl.handshake(conn)

        while True:
            event = conn.recv_cmd()
            result = dl.handle(conn, event)
            if result is not None:
                return result

    except connection.Closed, e:
        log.warning('{} at {}'.format(e, addr))

class Downloader:

    def __init__(self, host_id, info_hash):
        self.host_id = host_id
        self.info_hash = info_hash

    def get_peers(self, trackers):
        # return [('localhost', 51413)] # test local BT
        print trackers
        return tracker.get_peers(trackers[0], self.info_hash, self.host_id, num_want=50)

    def run(self, trackers):
        q = gevent.queue.Queue()
        def peer_loop(addr):
            q.put( (addr, loop(self, addr)) )

        greenlets = {}
        while True:

            if not greenlets:
                for addr in self.get_peers(trackers):
                    greenlets[addr] = gevent.spawn(peer_loop, addr)
                continue

            addr, result = q.get()
            greenlets.pop(addr)
            log.info('{} peers left'.format(len(greenlets)))
            if result:
                gevent.killall(greenlets.values())
                return result

class Metadata(Downloader):

    def handshake(self, conn):
        host_exts = set(peer.extensions[k] for k in ['commands'])
        peer_exts = conn.handshake(
            info_hash=self.info_hash, host_id=self.host_id, extensions=host_exts)
        assert host_exts.issubset(peer_exts)
        
        obj = OrderedDict(m=peer.extended_commands)
        conn.send_cmd('extended', cmd=0, msg=bencode.encode(obj))


    def handle(self, conn, event):

        if event.name in {'unchoke', 'choke'}:
            conn.state['am_choking'] = (event.name == 'choke')
            log.info('host is {}d by {}'.format(event.name, conn.name))

            metadata_request(conn, piece=0)
            conn.state['metadata'] = []
            return

        if event.name == 'extended':
            if event.cmd == peer.UT_METADATA:
                d, piece = bencode._decode(event.msg)

                if d['msg_type'] == 0: # request
                    log.warning('unsupported')

                if d['msg_type'] == 1: # data
                    log.info('got {} bytes of metadata from {}'.format(len(piece), conn.name))
                    pieces = conn.state['metadata']
                    pieces.append(piece)
                    data = ''.join(pieces)
                    if self.info_hash == metainfo.hash(data):
                        return metadata_save(data)
                    else:
                        metadata_request(conn, piece=d['piece']+1)

                if d['msg_type'] == 2: # reject
                    log.warning('no metadata')

            return


Request = namedtuple('Request', ['index', 'begin', 'length'])

def take(iterable, n):
    return list(itertools.islice(iterable, n))

class Torrent(Downloader):

    def __init__(self, host_id, meta):
        Downloader.__init__(self, host_id, meta.info_hash)
        self.data = storage.Data(meta)

        piece_indices = storage.indices(~self.data.bits) # indices to missing pieces

        self.queue_size = 2**3
        self.block_size = 2**14

        # requests data structure: maps request to peers it was sent to
        self.reqs = OrderedDict( (req, set()) for req in self._create_reqs(piece_indices) )
        log.debug('created {} requests'.format(len(self.reqs)))

    def handshake(self, conn):

        conn.handshake(info_hash=self.info_hash, host_id=self.host_id, extensions=[])
        conn.send_cmd('bitfield', bits=self.data.bits.tobytes())
        conn.send_cmd('unchoke')
        conn._pending_requests = set()

    def download(self, conn, choke=None, bits=None, piece=None):

        if choke is not None:
            # handle choke/unchoke message
            if choke:
                while conn._pending_requests:
                    req = conn._pending_requests.pop()
                    log.info('flush {} '.format(req))
                    peers = self.reqs.get(req)
                    if peers and conn.peer_id in peers:
                        peers.remove(conn.peer_id)

            else: # unchoke
                self._request(conn)

        if bits is not None: # handle bits/have message
            self._request(conn)

        if piece is not None: # handle piece message
            req = Request(piece.index, piece.begin, len(piece.data))
            conn._pending_requests.remove(req)
            peers = self.reqs.pop(req)
            assert peers == set([conn.peer_id]) # we expect only one request per block
            if all(r not in self.reqs for r in self._create_reqs([req.index])):
                # all requests of this piece are recieved
                self.data.validate([req.index])

            self._request(conn)

    def _request(self, conn):

        if conn.state['am_choking']:
            return # this peer is choked

        while len(conn._pending_requests) < self.queue_size:
            peer_bits = conn.state['peer_bits']
            bits = peer_bits & ~self.data.bits
            if not any(bits): 
                return # this peer has no useful pieces

            # requests for blocks the peer has, and the host does not, and were not requsted yet
            reqs = (r for r, peers in self.reqs.items() if bits[r.index] and len(peers) == 0)
            reqs = take(reqs, 1)
            if not reqs:
                return # no additional requests should be made

            req, = reqs
            log.debug('requesting #{} @ {} [{:.1f}kB] from peer {}'.format(req.index, req.begin, req.length / 1e3, conn.name))
            self.reqs[req].add( conn.peer_id )
            conn.send_cmd('request', **vars(req))
            conn._pending_requests.add(req)            

    def handle(self, conn, event):
        if event.name in {'unchoke', 'choke'}:
            conn.state['am_choking'] = (event.name == 'choke')
            log.info('host is {}d by peer {}'.format(event.name, conn.name))
            self.download( conn, choke=conn.state['am_choking'] )
            return

        if event.name == {'interested', 'uninterested'}:
            conn.state['peer_interested'] = (event.name == 'interested')
            log.info('peer {} is {}'.format(conn.name, event.name))
            return

        if event.name in {'bitfield', 'have'}:
            if event.name == 'bitfield':
                peer_bits = storage.bitfield(event.bits, n=len(self.data.meta.hashes))
            else: # have
                peer_bits = conn.state.get('peer_bits')
                if peer_bits is None:
                    peer_bits = storage.bitfield(None, n=len(self.data.meta.hashes))
                peer_bits[event.index] = True

            log.debug('peer {} has {} of {} pieces'.format(conn.name, 
                peer_bits.count(), peer_bits.length()))

            conn.state['peer_bits'] = peer_bits
            self.download( conn, bits=peer_bits )

            needed = peer_bits & ~self.data.bits
            log.debug('peer {} has {} needed pieces'.format(conn.name, needed.count()))

            if any(needed) and conn.state['am_interested'] == False:
                log.info('host is interested in peer {}'.format(conn.name))
                conn.state['am_interested'] = True
                conn.send_cmd('interested')
                return

            if not any(needed) and conn.state['am_interested'] == True:
                log.info('host is uninterested in peer {}'.format(conn.name))
                conn.state['am_interested'] = False
                conn.send_cmd('uninterested')
                return

            return

        if event.name == 'piece':
            log.debug('downloaded #{} @ {} [{:.1f}kB] from {}'.format(event.index, event.begin, len(event.data) / 1e3, conn.name))
            self.data.write(index=event.index, begin=event.begin, data=event.data)
            self.download( conn, piece=event )            
            if all(self.data.bits):
                return self.data # and stop download

            return

        if event.name == 'request':
            log.debug('peer {} request #{} @ {} [{:.1f}kB]'.format(conn.name, event.index, event.begin, event.length / 1e3))
            data = self.data.read(index=event.index, begin=event.begin, size=event.length)

            conn.send_cmd('piece', index=event.index, begin=event.begin, data=data)
            log.debug('peer {} upload  #{} @ {} [{:.1f}kB]'.format(conn.name, event.index, event.begin, len(data) / 1e3))
            return

        if event.name == 'keep_alive':
            log.debug('got keep alive from peer {}'.format(conn.name))
            return

        log.warning('unsupported event: {}'.format(event.name))

    def _create_reqs(self, piece_indices):
        for index in piece_indices:
            piece_size = self.data.piece_size(index)
            offset = 0
            while offset < piece_size:
                size = min(self.block_size, piece_size - offset)
                yield Request(index, offset, size)
                offset = offset + size
