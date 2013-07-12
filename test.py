import logging

import connection
import protocol
import metainfo
import storage
import tracker

import gevent

log = logging.getLogger('main')

def main(args):
    logging.basicConfig(
        format='%(asctime)-15s [%(levelname)s] %(message)s', 
        level=logging.DEBUG)

    meta = metainfo.MetaInfo(args.filename)
    data = storage.Data(meta)

    if any(~data.bits):
        host_id = metainfo.hash(args.host_id)
        peers = tracker.get_peers(meta, host_id, port=6889)
        for addr in peers:
            try:
                conn = connection.Connection(addr, timeout=10)
                p = protocol.PeerProtocol(host_id, data, conn=conn)
                p.loop()
            except connection.Closed:
                log.warning('connection closed by peer')
            except connection.Timeout:
                log.warning('peer timeout')

    log.info('Download is over')
    return

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Python BitTorrent Client')
    parser.add_argument('filename')
    parser.add_argument('host_id')
    main(parser.parse_args())
