import logging

import connection
import protocol
import metainfo
import storage
import tracker

import gevent
import gevent.pool

log = logging.getLogger('main')

def handle_peer(host_id, data, addr):
    try:
        conn = connection.Connection(addr, timeout=60)
        p = protocol.PeerProtocol(host_id, data, conn=conn)
        p.loop()
    except connection.Closed as e:
        log.warning('{} connection closed: {}'.format(addr, e))

def main(args):
    logging.basicConfig(
        format='%(asctime)-15s [%(levelname)s] %(name)s: %(message)s', 
        level=logging.INFO)

    meta = metainfo.MetaInfo(args.filename)
    data = storage.Data(meta)

    host_id = metainfo.hash(args.id)            
    pool = gevent.pool.Pool(args.pool)
    while any(~data.bits):
        peers = tracker.get_peers(meta, host_id, port=6889, timeout=10, num_want=-1)
        if peers:
            for addr in peers:
                pool.spawn(handle_peer, host_id, data, addr)
            pool.join()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Python BitTorrent Client')
    parser.add_argument('filename')
    parser.add_argument('--id')
    parser.add_argument('--pool', default=1, type=int)
    main(parser.parse_args())
