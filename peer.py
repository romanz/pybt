import construct as c


_handshake = c.Struct('handshake', 
    c.UBInt8('pstrlen'), 
    c.Bytes('pstr', lambda ctx: ctx.pstrlen),
    c.Bytes('reserved', 8),
    c.Bytes('info_hash', 20),
    c.Bytes('peer_id', 20))


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
}

for k, v in _commands.items():
    _commands[k] = c.Struct(k, *v)

def build_handshake(info_hash, peer_id, pstr=None, reserved=None):
    if pstr is None:
        pstr = 'BitTorrent protocol'

    if reserved is None:
        reserved = '\x00'*8

    obj = c.Container(info_hash=info_hash, peer_id=peer_id, 
                      pstr=pstr, pstrlen=len(pstr), reserved=reserved)
    return _handshake.build(obj)

def parse_handshake(msg):
    return _handshake.parse(msg)

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

def test():
    msg = build_handshake(info_hash='\x01'*20, peer_id='\x02'*20)
    print repr(parse_handshake(msg))

    msg = build_command('request', index=0x05, begin=0x06, length=0x01020304)
    print repr(parse_command(msg))

    msg = build_command('bitfield', bits='12345')
    print repr(parse_command(msg))


    msg = build_command('piece', index=0, begin=1, data='abcde')
    print repr(parse_command(msg))

if __name__ == '__main__':
    test()
