import logging

import metainfo
import download

log = logging.getLogger('main')

def main(args):
    logging.basicConfig(
        format='%(asctime)-15s [%(levelname)s] %(name)s: %(message)s', 
        level=logging.INFO)

    host_id = metainfo.hash(args.host)

    link = 'magnet:?xt=urn:btih:e1dfffe75594e5541f4e8bfd9245f20c1beb1003&dn=Adventure.Time.S05E27.HDTV.x264-W4F.mp4&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A80&tr=udp%3A%2F%2Ftracker.publicbt.com%3A80&tr=udp%3A%2F%2Ftracker.istole.it%3A6969&tr=udp%3A%2F%2Ftracker.ccc.de%3A80&tr=udp%3A%2F%2Fopen.demonii.com%3A1337'
    m = metainfo.parse_magnet(link)

    meta = download.metadata_load(m['info_hash'])
    if meta is None:
        dl = download.Metadata(host_id, m['info_hash'])
        meta = dl.run(m['trackers'])

    dl = download.Torrent(host_id, meta)
    if not all(dl.data.bits):
        dl.run(m['trackers'])
    log.info('Download completed')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Simple Python BitTorrent Client')
    parser.add_argument('--host', default='N/A')
    args = parser.parse_args()

    try:
        main(args)
    except KeyboardInterrupt:
        log.info('Program stopped via Ctrl-C')
