from argparse import ArgumentParser
import os
from xfuse.config import CACHE_DIR as DEFAULT_CACHE_DIR, DEFAULT_MNT_DIR, METADATA_DB_PATH as DEFAULT_DB_PATH, PIECE_DIR as DEFAULT_PIECE_DIR, TORRENT_DIR as DEFAULT_TORRENT_DIR, log, logging
from xfuse.module.fuse import FS
from xfuse.module.metadatadb import MetadataDB
from xfuse.module.scanner import Scanner
from fuse import FUSE

def main():
    parser = ArgumentParser()
    parser.add_argument('mountpoint', type=str, nargs='?', default=DEFAULT_MNT_DIR,
                        help='Where to mount the file system')
    parser.add_argument('--cache', type=str, default=DEFAULT_CACHE_DIR, help='Path to cache directory')
    parser.add_argument('--piece', type=str, default=DEFAULT_PIECE_DIR, help='Path to piece directory')
    parser.add_argument('--torrent', type=str, default=DEFAULT_TORRENT_DIR, help='Path to torrent directory')
    parser.add_argument('--db', type=str, default=DEFAULT_DB_PATH, help='Path to metadata database')
    parser.add_argument('--log-level', default='WARNING', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    parser.add_argument('--scanner-interval', type=int, default=60, help='Scanner check interval in seconds')
    parser.add_argument('--scanner-workers', type=int, default=4, help='Max concurrent splitting tasks')
    parser.add_argument('-o', '--fuse-opt', type=str, action='append', default=[],
                        help='FUSE options (e.g., -o allow_other,default_permissions)')
    parser.add_argument('--foreground', action='store_true', default=False,
                        help='Run FUSE in foreground (default: background)')
    args = parser.parse_args()

    log.setLevel(getattr(logging, args.log_level))
    file_handler = logging.FileHandler('error.log')
    file_handler.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)

    try:
        os.makedirs(args.mountpoint, exist_ok=True)
    except OSError as e:
        log.error(f"Failed to create mount point '{args.mountpoint}': {e}")
        exit(1)

    # 初始化数据库和TorrentFS
    db = MetadataDB(args.db)
    operations = FS(db, args.cache, args.piece, args.torrent, args.mountpoint, max_workers=args.scanner_workers)

    scanner = Scanner(operations, interval=args.scanner_interval)
    scanner.start()

    log.info(f"Mounting TorrentFS at {args.mountpoint}")
    fuse_opts_dict = {}
    for opt in args.fuse_opt:
        if '=' in opt:
            key, value = opt.split('=', 1)
            fuse_opts_dict[key] = value
        else:
            fuse_opts_dict[opt] = True
    fuse_opts_dict.setdefault('fsname', 'torrentfs')
    fuse_opts_dict.setdefault('subtype', 'torrentfs')
    fuse_opts_dict.setdefault('foreground', args.foreground)

    fuse = None
    try:
        fuse = FUSE(operations, args.mountpoint, big_writes=True, **fuse_opts_dict)
    except Exception as e:
        log.exception("An unexpected error occurred during FUSE mounting:")
        log.info("Stopping scanner thread due to mount failure...")
        scanner.stop()
        try:
            scanner.join(timeout=5)
        except Exception as join_e:
            log.error(f"Error joining scanner thread: {join_e}")
        exit(1)

    try:
        log.info("Filesystem mounted. Press Ctrl+C to unmount.")
    except KeyboardInterrupt:
        log.info("Received KeyboardInterrupt, initiating unmount...")
    finally:
        log.info("Stopping scanner thread...")
        scanner.stop()
        try:
            scanner.join(timeout=args.scanner_interval + 5)
            if scanner.is_alive():
                log.warning("Scanner thread did not stop gracefully.")
        except Exception as join_e:
            log.error(f"Error joining scanner thread: {join_e}")

        log.info("Cleanup sequence initiated. Exiting.")

if __name__ == '__main__':
    main()
