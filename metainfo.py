import math
import urllib
import hashlib
import bencode
import binascii

def split(data, block_size):
    n = len(data)
    for offset in range(0, n, block_size):
        yield data[offset : offset + block_size]

HASH_SIZE = 20
def piece_hashes(pieces):
    # Split into SHA1 hashes (160bits/hash)
    hashes = list(split(pieces, block_size=HASH_SIZE))
    assert all(len(h) == HASH_SIZE for h in hashes)
    return hashes

def hash(data): 
    return hashlib.sha1(data).digest()

class ParseError(Exception):
    pass

def parse_magnet(link):
    prefix = 'magnet:?'
    if not link.startswith(prefix):
        raise ParseError('Invalid magnet link {!r}'.format(link))

    args = link.split('?', 1)[1]
    args = args.split('&')

    result = {}
    for k, v in (arg.split('=') for arg in args):
        v = urllib.unquote(v)
        result.setdefault(k, []).append(v)

    name, = result['dn'] 

    urn, = result['xt']  
    info_hash = urn.rsplit(':', 1)[1]
    info_hash = binascii.unhexlify(info_hash)

    trackers = result['tr'] 
    
    return {'name': name, 'info_hash': info_hash, 'trackers': trackers}

class MetaInfo:
    def __init__(self, info):
        self.info_hash = hash(bencode.encode(info))
        self.name = info['name']
        if 'length' in info:
            self.total = info['length']
        if 'files' in info:
            self.total = sum(f['length'] for f in info['files'])

        self.hashes = piece_hashes(info['pieces'])
        self.piece_length = info['piece length']

        assert math.ceil(float(self.total) / self.piece_length) == len(self.hashes)

    def __repr__(self):
        return '<MetaInfo "{}" ({} hashes) = {:.1f}MB>'.format(self.name, 
            len(self.hashes), self.total / 1e6)
