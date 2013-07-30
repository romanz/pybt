import logging

import metainfo
import download

log = logging.getLogger('main')

def main(args):
    logging.basicConfig(
        format='%(asctime)-15s [%(levelname)s] %(name)s: %(message)s', 
        level=logging.INFO)

    host_id = metainfo.hash(args.host)

    m = metainfo.parse_magnet(args.link)
    meta = download.metadata_load(m['info_hash'])

    if args.metadata:
        if meta is None:
            dl = download.Metadata(host_id, m['info_hash'])
            meta = dl.run(m['trackers'])

    if args.torrent and meta:
        dl = download.Torrent(host_id, meta)
        if not all(dl.data.bits):
            dl.run(m['trackers'])
        log.info('Download completed')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Simple Python BitTorrent Client')
    parser.add_argument('--host', default='none')
    parser.add_argument('--link', default='')
    parser.add_argument('--metadata', action='store_true', default=False)
    parser.add_argument('--torrent', action='store_true', default=False)
    args = parser.parse_args()

    try:
        main(args)
    except KeyboardInterrupt:
        log.info('Program stopped via Ctrl-C')
