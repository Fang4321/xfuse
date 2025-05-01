import hashlib
import os
import stat
import threading
from xfuse.config import log
from xfuse.module.utils import parse_torrent_file


class Scanner(threading.Thread):
    def __init__(self, fs_instance, interval=60): # Pass TorrentFS instance
        """Initializes the Scanner thread."""
        super().__init__(daemon=True)
        self.fs = fs_instance # Store TorrentFS instance to access shared resources
        self.mountpoint = fs_instance.mountpoint
        self.interval = interval
        self._stop_event = threading.Event()
        log.info("Scanner initialized (uses shared executor).")

    def stop(self):
        self._stop_event.set()
        log.info("Scanner stop requested.")

    def _read_piece(self,file_list, cache_root, piece_length, index):
            """
            Reads data for the piece at the given index, handling file boundaries
            according to BT client logic.
            """
            start = index * piece_length
            remaining = piece_length
            offset = start
            buf = bytearray()
            for rel, length in file_list:
                path = os.path.join(cache_root, rel)
                if offset >= length:
                    offset -= length
                    continue
                to_read = min(length - offset, remaining)
                try:
                    with open(path, 'rb') as f:
                        f.seek(offset)
                        buf.extend(f.read(to_read))
                except Exception as e:
                    log.error(f"read_piece error on {path}: {e}")
                    return None
                remaining -= to_read
                if remaining == 0:
                    break
                offset = 0
            return bytes(buf)

    def run(self):
        log.info("Scanner thread started.")
        while not self._stop_event.is_set():
            try:
                for fname in os.listdir(self.fs.torrent_dir):
                    if not fname.endswith('.torrent'):
                        continue
                    info = parse_torrent_file(os.path.join(self.fs.torrent_dir, fname))
                    if not info:
                        continue
                    name = info['name']
                    files = info['files']
                    piece_length = info['piece_length']
                    pieces = info['pieces']
                    is_multi = info.get('is_multi', any('/' in f['path'] for f in files))
                    base_cache = os.path.join(self.fs.cache_dir, name) if is_multi else self.fs.cache_dir
                    base_mount = os.path.join(self.mountpoint, name) if is_multi else self.mountpoint
                    file_list = [(e['path'], e['length']) for e in files]

                    # Verify all pieces
                    ok = True
                    for idx, expected in enumerate(pieces):
                        data = self._read_piece(file_list, base_cache, piece_length, idx)
                        if data is None:
                            ok = False
                            break
                        actual = hashlib.sha1(data).digest()
                        if actual != expected:
                            log.warning(f"Piece {idx} mismatch in {name}")
                            ok = False
                            break
                    if not ok:
                        continue

                    # If all pieces verified, mark as read-only and trigger split
                    if is_multi:
                        target = base_mount               
                    else:
                        target = os.path.join(base_mount, files[0]['path'])

                    try:
                        st  = os.stat(target)
                        new = stat.S_IMODE(st.st_mode) & ~0o222
                        # Trigger batch splitting or single file splitting via chmod
                        os.chmod(target, new)     
                        log.info(f"Scanner: chmod a-w {target}")
                    except Exception as e:
                        log.error(f"Scanner: chmod {target} failed: {e}")
                    try:
                        os.remove(os.path.join(self.fs.torrent_dir, fname))
                        log.info(f"Scanner: removed verified torrent {fname}")
                    except OSError as e:
                        log.warning(f"Scanner: failed to delete torrent {fname}: {e}")
            except Exception:
                log.exception("Error in scanner cycle")
            self._stop_event.wait(self.interval)
        log.info("Scanner thread stopped.")

