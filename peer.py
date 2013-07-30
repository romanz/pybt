import collections
import construct as c
import bitarray
import logging

_PSTR = 'BitTorrent protocol'
_HANDSHAKE_PREFIX = chr(len(_PSTR)) + _PSTR # length-prefixed string

_handshake = c.Struct('handshake', 
    c.Magic(_HANDSHAKE_PREFIX),
    c.Bytes('reserved', 8),     # 64 bitfield
    c.Bytes('info_hash', 20),   # 160 bit hash
    c.Bytes('peer_id', 20))     # 160 bit hash

# Reserved bitfield options
extensions = { 
    'commands': 20, # support for Extension Protocol (BEP 0010)
}

# support for Peer Metadata Exchange (BEP 0009)
UT_METADATA = 3
extended_commands = collections.OrderedDict(ut_metadata=UT_METADATA)

_message = c.Struct('message', 
        c.UBInt32('length'), 
        c.Bytes('payload', lambda ctx: ctx.length))

Bytes = lambda name: c.ExprAdapter(c.OptionalGreedyRange(c.StaticField(name, 1)), 
    encoder=lambda obj, ctx : list(obj),
    decoder=lambda obj, ctx : ''.join(obj)
)

_commands = {
    'choke'         : [c.Magic('\x00')],
    'unchoke'       : [c.Magic('\x01')],
    'interested'    : [c.Magic('\x02')],
    'uninterested'  : [c.Magic('\x03')],
    'have'          : [c.Magic('\x04'), c.UBInt32('index')],
    'bitfield'      : [c.Magic('\x05'), Bytes('bits')],
    'request'       : [c.Magic('\x06'), c.UBInt32('index'), c.UBInt32('begin'), c.UBInt32('length')],
    'piece'         : [c.Magic('\x07'), c.UBInt32('index'), c.UBInt32('begin'), Bytes('data')],
    'cancel'        : [c.Magic('\x08'), c.UBInt32('index'), c.UBInt32('begin'), c.UBInt32('length')],
    'port'          : [c.Magic('\x09'), c.UBInt16('port')],
    'extended'      : [c.Magic('\x14'), c.UBInt8('cmd'), Bytes('msg')],
}

for k, v in _commands.items():
    _commands[k] = c.Struct(k, *v)

def build_handshake(info_hash, host_id, extensions):

    bits = bitarray.bitarray([0]*64, endian='little')
    for i in extensions:
        bits[i] = True

    obj = c.Container(info_hash=info_hash, peer_id=host_id, 
                      reserved=bits.tobytes())

    return _handshake.build(obj)

def parse_handshake(msg):
    obj = _handshake.parse(msg)
    bits = bitarray.bitarray()
    bits.frombytes(obj.reserved)
    obj.extensions = [i for i, b in enumerate(reversed(bits)) if b]
    return obj

def build_command(name, **kw):    
    if name == 'keep_alive':
        return _message.build(c.Container(length=0, payload=''))

    s = _commands[name]
    obj = c.Container(**kw)
    payload = s.build(obj)

    return _message.build(c.Container(length=len(payload), payload=payload))

def parse_command(msg):
    obj = _message.parse(msg)
    cmd = obj.payload
    if not cmd:
        return c.Container(name='keep_alive')

    for k, v in _commands.items():
        try:            
            obj = v.parse(cmd)
            obj.name = k
            return obj
        except c.ConstructError:
            pass
    raise ParseError('cannot parse command')

log = logging.getLogger('peer')

class Connection:
    
    def __init__(self, conn):
        self.conn = conn
        self.name = '<{}:{}>'.format(*conn.sock.getpeername())
        self.peer_id = ''
        self.state = {}

    def recv_cmd(self):
        lentype = c.UBInt32('length')
        L = self.conn.recv(lentype.sizeof())
        n = lentype.parse(L)
        data = self.conn.recv(n)
        msg = L + data
        return parse_command(msg)
    
    def send_cmd(self, name, **kw):
        msg = build_command(name, **kw)
        self.conn.send(msg)

    def handshake(self, **kw):
        log.info('handshake to {} (extensions: {})'.format(
            self.name, kw['extensions']))

        msg = build_handshake(**kw)
        self.conn.send(msg)

        msg = self.conn.recv(len(msg)) # Assume reply message has the same length
        reply = parse_handshake(msg)

        assert reply.info_hash == kw['info_hash']

        log.info('handshake from {} (extensions: {})'.format(
            self.name, reply.extensions))
        log.info('{} id={!r}'.format(self.name, reply.peer_id))

        self.peer_id = reply.peer_id
        return reply.extensions

    def valid(self):
        return self.conn

    def close(self):
        self.conn.close()
        
    
def test():
    msg = build_handshake(info_hash='\x01'*20, host_id='\x02'*20)
    print repr(parse_handshake(msg))

    msg = build_command('request', index=0x05, begin=0x06, length=0x01020304)
    print repr(parse_command(msg))

    msg = build_command('bitfield', bits='12345')
    print repr(parse_command(msg))

    msg = build_command('piece', index=0, begin=1, data='abcde')
    print repr(parse_command(msg))

if __name__ == '__main__':
    test()
