import re
import urllib

def parse(magnet_link):
    prefix = 'magnet:?'
    assert magnet_link.startswith(prefix)
    args = magnet_link.split('?', 1)[1]
    args = args.split('&')

    result = {}
    for k, v in (arg.split('=') for arg in args):
        v = urllib.unquote(v)
        result.setdefault(k, []).append(v)

    filename, = result['dn'] 
    urn, = result['xt']  
    info_hash = urn.rsplit(':', 1)[1]
    trackers = result['tr'] 
    
    return {'filename': filename, 'hash': info_hash, 'trackers': trackers}
