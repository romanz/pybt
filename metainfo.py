import hashlib
import bencode

def load(fname):
    with file(fname, 'rb') as f:
        return bencode.decode(f.read())

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

class MetaInfo:
    def __init__(self, torrent_fname):
        _meta = load(torrent_fname)
        self.info_hash = hash(bencode.encode(_meta['info']))
        self.announce_addr = _meta['announce']
        self.name = _meta['info']['name']
        self.length = _meta['info']['length']
        self.hashes = piece_hashes(_meta['info']['pieces'])
        self.piece_length = _meta['info']['piece length']

