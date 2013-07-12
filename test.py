import logging

import connection
import protocol
import metainfo
import storage

def main(args):
    logging.basicConfig(
        format='%(asctime)-15s [%(levelname)s] %(message)s', 
        level=logging.DEBUG)

    meta = metainfo.MetaInfo(args.filename)
    host_id = metainfo.hash(args.host_id)

    data = storage.Data(meta)
    if any(~data.bits):
        conn = connection.Connection(('127.0.0.1', 51413), timeout=60)
        p = protocol.PeerProtocol(host_id, data, conn=conn)
        p.loop()

    log.info('Download is over')
    return

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Python BitTorrent Client')
    parser.add_argument('filename')
    parser.add_argument('host_id')
    main(parser.parse_args())
