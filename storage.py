import os
import math
import binascii
import logging

import bitarray

import metainfo

def bitfield(meta, data=None):
    total_size = meta.length
    n = int(math.ceil(float(total_size) / meta.piece_length))
    if data is None:
        bits = bitarray.bitarray(0 for i in xrange(n))
    else:
        bits = bitarray.bitarray(endian='big')
        bits.frombytes(data)
        assert not any(bits[n:])
        del bits[n:]
    return bits

def indices(bits):
    return [i for i, b in enumerate(bits) if b]

log = logging.getLogger('storage')

class Data:

    def __init__(self, meta):
        
        h = binascii.hexlify(meta.info_hash)
        log.info('torrent name: {}'.format(meta.name))        
        log.info('info hash: {}'.format(h))
        fname = '{}.tmp'.format( h )
        mode = ('r' if os.path.exists(fname) else 'w') + 'b+'
        self.fd = file(fname, mode)
        self.meta = meta
        self._fill()

        self.bits = bitarray.bitarray([0]*len(meta.hashes), endian='big')
        self.validate()

    def _fill(self):
        self.fd.seek(0, 2) # seek to EOF
        left = self.meta.length - self.fd.tell() # bytes left to fill
        buff = '\x00' * 1024
        while left > 0:
            buff = buff[:left]
            self.fd.write(buff)
            left = left - len(buff)
        self.fd.seek(self.meta.length)
        self.fd.truncate()

    def validate(self, index_list=None):
        if index_list is None:
            index_list = indices(~self.bits)

        for i in index_list:
            data = self.read(i)
            h = metainfo.hash(data)
            success = (self.meta.hashes[i] == h)
            self.bits[i] = success
            if success:
                log.debug('validated piece #{}'.format(i))

        log.info('host has {} of {} pieces'.format(self.bits.count(), len(self.bits)))
        return sum(self.bits[i] for i in index_list)

    def piece_size(self, index):
        n = len(self.bits)
        if index < 0 or index >= n:
            return 0
        if index < n-1:
            return self.meta.piece_length
        else:
            return self.meta.length - self.meta.piece_length * (n-1)

    def read(self, index, begin=0, size=None):
        if size is None:
            size = self.piece_size(index) # read all piece

        assert begin >= 0
        assert size >= 0
        assert begin + size <= self.piece_size(index)

        self.fd.seek(index * self.meta.piece_length + begin)
        data = self.fd.read(size)
        assert len(data) == size
        return data

    def write(self, index, begin, data):
        assert begin >= 0
        assert begin + len(data) <= self.piece_size(index)

        self.fd.seek(index * self.meta.piece_length + begin)
        self.fd.write(data)

