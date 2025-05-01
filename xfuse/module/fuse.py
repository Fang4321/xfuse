import errno
import hashlib
import os
import shutil
import stat
import threading
import time
from fuse import Operations, FuseOSError
import concurrent.futures
from xfuse.config import log
from xfuse.module.utils import read_piece, split_file_into_pieces


class FS(Operations):
    def __init__(self, db, cache_dir, piece_dir, torrent_dir,mountpoint, max_workers=4):
        """Initializes the TorrentFS instance."""
        self.db = db
        self.cache_dir = cache_dir
        self.piece_dir = piece_dir
        self.torrent_dir = torrent_dir
        self.mountpoint = mountpoint
        # File handle management
        self._file_handles = {} # {fh: {'path': str, 'inode': int, 'flags': int, 'cache_fd': int|None, 'file_hash': str|None}}
        self._next_fh = 0
        self._fh_lock = threading.Lock()
        # Shared ThreadPoolExecutor for splitting tasks
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        # Tracking active splitting tasks to prevent duplicates
        self.active_splitting_tasks = {} # {path: future}
        self.splitting_lock = threading.Lock() # Protects active_splitting_tasks dict
        # Ensure base directories exist
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.piece_dir, exist_ok=True)
        os.makedirs(self.torrent_dir, exist_ok=True)
        self._reconstruct_from_metadata()
        self._schedule_pending_readonly_splits()
        log.info(f"TorrentFS initialized with max {max_workers} workers.")

    def destroy(self, path):
        """Called on filesystem unmount."""
        log.info("Filesystem unmounting. Shutting down executor...")
        # Ensure executor is shut down cleanly
        self.executor.shutdown(wait=True)
        log.info("Executor shutdown complete.")

    def _get_real_path(self, path_in_fs):
        """Helper to get the corresponding path in the cache directory."""
        if path_in_fs.startswith('/'): path_in_fs = path_in_fs[1:]
        return os.path.join(self.cache_dir, path_in_fs)

    def _get_next_fh(self):
        """Generates the next file handle ID."""
        with self._fh_lock: self._next_fh += 1; return self._next_fh

    # --- Splitting Task Helpers (now methods of TorrentFS) ---
    def _run_splitting_async(self, path, cache_path, piece_dir, file_hash, file_size):
        """Actual splitting task run by the executor."""
        log.debug(f"Worker thread starting split for: {path}")
        try:
            # Call the global split_file_into_pieces function
            result = split_file_into_pieces(cache_path, piece_dir, file_hash, file_size)
            if result:
                piece_count, piece_size = result
                log.debug(f"Worker thread finished split successfully for: {path}")
                # Return results including the cache path to delete
                return piece_count, piece_size, cache_path
            else:
                log.error(f"split_file_into_pieces failed for {cache_path} in worker thread.")
                return None
        except Exception as e:
            log.exception(f"Exception during split_file_into_pieces for {cache_path} in worker thread:")
            raise # Propagate exception to callback

    def _split_task_done_callback(self, future):
        """Callback executed when splitting task finishes."""
        path = None
        exception = future.exception()

        # Find path associated with the future and remove it
        with self.splitting_lock:
            path_found = None
            # Use items() for safe iteration while potentially modifying dict size
            for p, f in list(self.active_splitting_tasks.items()):
                if f == future:
                    path_found = p
                    del self.active_splitting_tasks[p] # Remove task from tracking
                    break
            if path_found:
                path = path_found
                log.debug(f"Removed completed/failed splitting task for path: {path}")
            else:
                # This might happen if the task was cancelled or removed elsewhere
                log.warning("Could not find path associated with completed future in callback. Task might have been removed.")
                return

        if exception:
            log.error(f"Asynchronous splitting task for {path} failed: {exception}")
            # Optional: Mark file as error in DB?
            # try: self.db.update_status(path, is_complete=-1) # Example: -1 for error state
            # except Exception as e: log.error(f"Failed to mark path {path} as error in DB: {e}")
        else:
            result = future.result()
            if result:
                piece_count, piece_size, cache_path_to_delete = result
                log.info(f"Asynchronous splitting task for {path} completed successfully. Pieces: {piece_count}, Size: {piece_size}")
                try:
                    # Update DB: set piece info and mark as complete
                    if self.db.update_piece_info(path, piece_count, piece_size, is_complete=True):
                        log.info(f"Updated metadata for {path} with piece info and marked complete.")
                        # Delete original cache file
                        try:
                            os.remove(cache_path_to_delete)
                            log.info(f"Removed original cache file: {cache_path_to_delete}")
                        except FileNotFoundError:
                            log.warning(f"Cache file {cache_path_to_delete} not found for removal (already deleted?).")
                        except OSError as e:
                            log.warning(f"Error removing cache file {cache_path_to_delete} after splitting: {e}")
                    else:
                        log.error(f"Failed to update metadata for {path} after successful split.")
                except Exception as db_update_e:
                    log.exception(f"Error updating DB or removing cache file for {path} after split:")
            else:
                log.error(f"Asynchronous splitting task for {path} reported failure (result is None).")

    def _schedule_pending_readonly_splits(self):
        """Enqueue split tasks for DB entries that are read-only yet incomplete."""
        rows = self.db.find_readonly_incomplete_files()
        if not rows:
            return
        log.info(f"Init: scheduling {len(rows)} read-only incomplete files for splitting")
        for r in rows:
            path, cache_path, size, file_hash = r['path'], self._get_real_path(r['path']), r['size'], r['file_hash']
            if not cache_path or not os.path.exists(cache_path):
                continue
            if size == 0:
                try:
                    size = os.path.getsize(cache_path)
                except OSError:
                    continue
            if not file_hash:
                file_hash = hashlib.md5(path.encode('utf-8')).hexdigest()

            with self.splitting_lock:
                if path in self.active_splitting_tasks:
                    continue
                fut = self.executor.submit(
                    self._run_splitting_async,
                    path, cache_path, self.piece_dir, file_hash, size
                )
                fut.add_done_callback(self._split_task_done_callback)
                self.active_splitting_tasks[path] = fut

    def _reconstruct_from_metadata(self):
        """
        Reconstructs the cache directory structure based on metadata.
        Creates directories for active directory entries if they don't exist.
        Removes DB entries for active non-directory entries if the corresponding cache file is missing.
        Populates DB with entries found in the cache directory but not in the DB.
        """
        try:
             # Process active entries from DB
            with self.db._cursor() as cur:
                cur.execute(
                    "SELECT path, is_dir, is_complete "
                    "FROM metadata WHERE is_active = 1")
                rows = cur.fetchall()

            for row in rows:
                path        = row["path"]
                is_dir      = bool(row["is_dir"])
                is_complete = bool(row["is_complete"])
                cache_path  = self._get_real_path(path)

                # -------- Directories: Ensure existence --------
                if is_dir:
                    # Root directory doesn't need processing
                    if path == "/":
                        continue
                    # Calculate cache_path if not stored in DB (older versions)
                    os.makedirs(cache_path, exist_ok=True)

                # -------- Files: Only check 'incomplete files' --------
                else:
                    # Only care about incomplete files; completed files are in piece dir, not checked here
                    if is_complete:
                        continue                    
                    # Check if the actual file exists. If not, remove from DB.
                    if not os.path.exists(cache_path):
                        self.db.delete_entry(path)
                        log.info(
                            f"Init-cleanup: removed stale DB entry {path}")
        except Exception:
            log.exception(
                "Init-cleanup: error while reconstructing cache structure")
        
        try:
            for root, dirs, files in os.walk(self.cache_dir):
                # Calculate virtual path prefix (path visible within the mount)
                vprefix = "/" + os.path.relpath(root, self.cache_dir).lstrip(".")
                if vprefix == "/.":
                    vprefix = "/"

                # ---- 3.1 Directories ----
                for d in dirs:
                    vpath = os.path.join(vprefix, d) if vprefix != "/" else f"/{d}"
                    if not self.db.get_entry_by_path(vpath):
                        mode = stat.S_IFDIR | 0o755
                        db_parent = os.path.dirname(vpath)
                        if db_parent != "/" and not db_parent.endswith("/"):
                            db_parent += "/"
                        self.db.add_entry(db_parent, d, vpath, True, mode)
                        log.debug(f"Init-add dir {vpath}")

                # ---- 3.2 Files ----
                for f in files:
                    vpath = os.path.join(vprefix, f) if vprefix != "/" else f"/{f}"
                    if not self.db.get_entry_by_path(vpath):
                        fp = os.path.join(root, f)
                        st = os.stat(fp)
                        mode_bits = stat.S_IFREG | (st.st_mode & 0o777)
                        db_parent = os.path.dirname(vpath)
                        if db_parent != "/" and not db_parent.endswith("/"):
                            db_parent += "/"
                        self.db.add_entry(db_parent, f, vpath, False, mode_bits,
                                          size=st.st_size,
                                          torrent_path=None,
                                          file_hash=hashlib.md5(vpath.encode()).hexdigest())
                        log.debug(f"Init-add file {vpath}")
        except Exception:
            log.exception("Init-populate stage failed while scanning cache")

    # --- FUSE Operations ---
    def getattr(self, path, fh=None):
        log.debug(f"getattr called for path: {path}")
        entry = self.db.get_entry_by_path(path)
        if not entry or not entry['is_active']:
            raise FuseOSError(errno.ENOENT)
        
        attrs = {
            'st_ino': entry['inode'],
            'st_mode': entry['mode'],
            'st_nlink': 1, # Typically 1 for files, 2+ for dirs
            'st_uid': entry['uid'],
            'st_gid': entry['gid'],
            'st_rdev': 0,
            'st_atime': entry['atime'],
            'st_mtime': entry['mtime'],
            'st_ctime': entry['ctime'],
        }
        
        if entry['is_dir']:
            attrs['st_size'] = entry['size'] if entry['size'] else 4096 # Default dir size
        elif entry['is_complete']:
            attrs['st_size'] = entry['size']
            # Ensure mode reflects read-only status for completed files
            attrs['st_mode'] &= ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
        else: # Incomplete file
            cache_path = self._get_real_path(path)
            current_size = entry['size'] # Start with DB size
            if cache_path and os.path.exists(cache_path):
                try:
                    stat_res = os.stat(cache_path)
                    # Just use the cache file's size directly, no DB update
                    attrs['st_size'] = stat_res.st_size
                    attrs['st_mtime'] = stat_res.st_mtime
                    attrs['st_atime'] = stat_res.st_atime
                    # Removed the DB update logic as requested
                except FileNotFoundError:
                    log.warning(f"getattr: Cache file {cache_path} gone for {path}. Using DB size {entry['size']}.")
                    attrs['st_size'] = entry['size']
                except Exception as e:
                    log.error(f"getattr: Error stating cache file {cache_path}: {e}")
                    attrs['st_size'] = entry['size']
            else:
                attrs['st_size'] = current_size
        
        # log.debug(f"getattr for path {path}: mode={oct(attrs['st_mode'])}, size={attrs['st_size']}")
        return attrs

    def readdir(self, path, fh):
        # (Implementation same as v2)
        log.debug(f"readdir called for path: {path}")
        parent_entry = self.db.get_entry_by_path(path)
        if not parent_entry or not parent_entry['is_dir'] or not parent_entry['is_active']:
            raise FuseOSError(errno.ENOENT)
        children_names = self.db.get_children(path)
        dir_list = ['.', '..'] + children_names
        # log.debug(f"readdir returning: {dir_list}")
        return dir_list

    def mkdir(self, path, mode):
        # (Implementation mostly same as v2, ensure parent times updated)
        log.info(f"mkdir called for path='{path}', mode={oct(mode)}")
        if path == '/': raise FuseOSError(errno.EEXIST)
        parent_path = os.path.dirname(path); name = os.path.basename(path)
        if not name: raise FuseOSError(errno.ENOENT)
        parent_entry = self.db.get_entry_by_path(parent_path)
        if not parent_entry or not parent_entry['is_dir'] or not parent_entry['is_active']: raise FuseOSError(errno.ENOENT)
        existing = self.db.get_entry_by_path(path)
        if existing and existing['is_active']: raise FuseOSError(errno.EEXIST)

        cache_path = self._get_real_path(path)
        try:
            current_umask = os.umask(0); os.umask(current_umask)
            actual_mode = mode & ~current_umask
            os.makedirs(cache_path, mode=actual_mode, exist_ok=True)
            log.info(f"Created directory in cache: {cache_path} with mode {oct(actual_mode)}")
        except OSError as e: log.error(f"Failed to create cache directory {cache_path}: {e}"); raise FuseOSError(e.errno)

        try:
            dir_mode = stat.S_IFDIR | mode
            db_parent_path = parent_path
            if db_parent_path != '/' and not db_parent_path.endswith('/'): db_parent_path += '/'
            self.db.add_entry(db_parent_path, name, path, is_dir=True, mode=dir_mode)
            log.info(f"Added directory to metadata: path={path}")
            # Update parent directory's mtime and ctime
            self.db.update_entry(parent_path, mtime=time.time(), ctime=time.time())
            return 0
        except Exception as e:
            log.exception(f"Failed to add directory {path} to metadata:");
            try: 
                os.rmdir(cache_path); 
            except OSError: pass; raise FuseOSError(errno.EIO)

    def create(self, path, mode, fi=None):
        # (Implementation mostly same as v2, ensure parent times updated)
        log.info(f"create called for path='{path}', mode={oct(mode)}")
        if path == '/': raise FuseOSError(errno.EPERM)
        parent_path = os.path.dirname(path); name = os.path.basename(path)
        if not name: raise FuseOSError(errno.ENOENT)
        parent_entry = self.db.get_entry_by_path(parent_path)
        if not parent_entry or not parent_entry['is_dir'] or not parent_entry['is_active']: raise FuseOSError(errno.ENOENT)
        existing = self.db.get_entry_by_path(path)
        if existing and existing['is_active']: raise FuseOSError(errno.EEXIST)

        cache_path = self._get_real_path(path)
        cache_fd = -1
        try:
            current_umask = os.umask(0); os.umask(current_umask)
            actual_mode = mode & ~current_umask
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            create_flags = os.O_CREAT | os.O_EXCL | os.O_RDWR # Use RDWR for create
            cache_fd = os.open(cache_path, create_flags, actual_mode)
            log.info(f"Created and opened cache file: {cache_path} with mode {oct(actual_mode)}, fd={cache_fd}")
        except OSError as e:
            log.error(f"Failed to create/open cache file {cache_path}: {e}")
            if cache_fd != -1: os.close(cache_fd)
            raise FuseOSError(e.errno)

        try:
            file_mode = stat.S_IFREG | mode
            db_parent_path = parent_path
            if db_parent_path != '/' and not db_parent_path.endswith('/'): db_parent_path += '/'
            # Add entry with size 0 initially, write/scanner updates later
            inode = self.db.add_entry(db_parent_path, name, path, is_dir=False, mode=file_mode, size=0)
            log.info(f"Added file to metadata: path={path}, inode={inode}")

            fh = self._get_next_fh()
            self._file_handles[fh] = {'path': path, 'inode': inode, 'flags': create_flags, 'cache_fd': cache_fd}
            log.debug(f"Assigned fh={fh} for new file path={path}")
            # Update parent directory's mtime and ctime
            self.db.update_entry(parent_path, mtime=time.time(), ctime=time.time())
            return fh
        except Exception as e:
            log.exception(f"Failed to add file {path} to metadata:")
            if cache_fd != -1: 
                try: 
                    os.close(cache_fd); 
                    os.remove(cache_path); 
                except OSError: pass
            raise FuseOSError(errno.EIO)

    def open(self, path, flags):
        # (Implementation mostly same as v2)
        log.debug(f"open called for path={path}, flags={oct(flags)}")
        entry = self.db.get_entry_by_path(path)
        if not entry or entry['is_dir'] or not entry['is_active']: raise FuseOSError(errno.ENOENT)

        is_writing = flags & (os.O_WRONLY | os.O_RDWR)

        if entry['is_complete'] and is_writing:
            log.warning(f"Attempt to open completed (read-only) file path={path} for writing. Denied.")
            raise FuseOSError(errno.EROFS)

        cache_fd = None
        if not entry['is_complete']:
            cache_path = self._get_real_path(path)
            if not cache_path: raise FuseOSError(errno.EIO)
            if not os.path.exists(cache_path):
                 log.error(f"open: Cache path {cache_path} does not exist for incomplete path {path}")
                 raise FuseOSError(errno.ENOENT)
            try:
                cache_fd = os.open(cache_path, flags)
                log.debug(f"Opened cache file {cache_path} for path={path}, fd={cache_fd}, flags={oct(flags)}")
            except OSError as e: log.error(f"Failed to open cache file {cache_path} for path={path}: {e}"); raise FuseOSError(e.errno)
        else: # Completed file
             log.debug(f"Opening completed file path={path} (flags={oct(flags)}, uses pieces)")

        fh = self._get_next_fh()
        self._file_handles[fh] = {'path': path, 'inode': entry['inode'], 'flags': flags, 'cache_fd': cache_fd, 'file_hash': entry['file_hash']}
        log.debug(f"Assigned fh={fh} for opened path={path}. Cache fd: {cache_fd}, Hash: {entry['file_hash']}")
        return fh

    def read(self, path, size, offset, fh):
        # (Implementation mostly same as v2)
        # log.debug(f"read called for path={path}, fh={fh}, offset={offset}, size={size}") # Verbose
        if fh not in self._file_handles: raise FuseOSError(errno.EBADF)
        handle_info = self._file_handles[fh]
        if handle_info['path'] != path: log.error(f"Read path mismatch!"); raise FuseOSError(errno.EIO)

        entry = self.db.get_entry_by_path(path)
        if not entry or not entry['is_active'] or entry['is_dir']: raise FuseOSError(errno.ENOENT)

        if entry['is_complete']:
            # Read from pieces
            file_hash = hashlib.md5(path.encode('utf-8')).hexdigest()
            piece_count = entry['piece_count']; piece_size = entry['piece_size']; total_size = entry['size']
            if not file_hash or piece_count is None or piece_size is None or total_size is None:
                log.error(f"read: Missing piece info for completed file {path}"); raise FuseOSError(errno.EIO)
            if offset >= total_size: return b''
            effective_size = min(size, total_size - offset); data = bytearray()
            if effective_size <= 0: return b''

            remaining_size = effective_size; current_offset = offset
            while remaining_size > 0 and current_offset < total_size:
                if piece_size <= 0: log.error(f"read: Invalid piece_size {piece_size}"); raise FuseOSError(errno.EIO)
                piece_index = current_offset // piece_size
                offset_in_piece = current_offset % piece_size
                read_size_in_piece = min(remaining_size, piece_size - offset_in_piece, total_size - current_offset)
                if read_size_in_piece <= 0: break
                try:
                    piece_data = read_piece(self.piece_dir, file_hash, piece_index, offset_in_piece, read_size_in_piece)
                    if not piece_data and read_size_in_piece > 0: log.warning(f"read: No data from piece {piece_index} for {path}. End?"); break
                    data.extend(piece_data); current_offset += len(piece_data); remaining_size -= len(piece_data)
                    if len(piece_data) < read_size_in_piece: log.warning(f"read: Short read from piece {piece_index}."); break
                except Exception as e: log.exception(f"read: Error reading piece {piece_index} for {path}"); raise FuseOSError(errno.EIO)
            # log.debug(f"Read {len(data)} bytes from pieces for fh={fh}")
            return bytes(data)
        else:
            # Read from cache file
            cache_fd = handle_info.get('cache_fd')
            if cache_fd is None: log.error(f"read: No cache fd for incomplete file fh={fh}"); raise FuseOSError(errno.EIO)
            #handle_flags = handle_info.get('flags', 0)
            #if not (handle_flags & (os.O_RDONLY | os.O_RDWR)): raise FuseOSError(errno.EBADF)
            # log.debug(f"Reading incomplete file path={path} from cache fd={cache_fd}")
            try:
                data = os.pread(cache_fd, size, offset)
                # log.debug(f"Read {len(data)} bytes from cache for fh={fh}")
                return data
            except OSError as e: log.error(f"Error reading from cache fd={cache_fd}: {e}"); raise FuseOSError(e.errno)

    def write(self, path, buf, offset, fh):
        # (Implementation mostly same as v2)
        # log.debug(f"write called for path={path}, fh={fh}, offset={offset}, len(buf)={len(buf)}") # Verbose
        if fh not in self._file_handles: raise FuseOSError(errno.EBADF)
        handle_info = self._file_handles[fh]
        if handle_info['path'] != path: log.error(f"Write path mismatch!"); raise FuseOSError(errno.EIO)

        cache_fd = handle_info.get('cache_fd')
        if cache_fd is None: log.error(f"write: No cache fd for incomplete file fh={fh}"); raise FuseOSError(errno.EIO)
        handle_flags = handle_info.get('flags', 0)
        if not (handle_flags & (os.O_WRONLY | os.O_RDWR)): raise FuseOSError(errno.EBADF)

        try:
            bytes_written = os.pwrite(cache_fd, buf, offset)
            return bytes_written
        except OSError as e: log.error(f"Error writing to cache fd={cache_fd}: {e}"); raise FuseOSError(e.errno)

    def release(self, path, fh):
        # (Implementation same as v2)
        log.debug(f"release called for path={path}, fh={fh}")
        if fh not in self._file_handles: log.warning(f"release: fh={fh} not found."); return 0
        handle_info = self._file_handles.pop(fh)
        cache_fd = handle_info.get('cache_fd')
        if cache_fd is not None:
            log.debug(f"Closing cache fd={cache_fd} for path={path}")
            try: os.fsync(cache_fd)
            except OSError as e:
                 if e.errno != errno.EBADF: log.error(f"Error closing cache fd={cache_fd}: {e}")
            except Exception as e: log.exception(f"Unexpected error during cache fd close for {path}")
            try: os.close(cache_fd)
            except OSError as e:
                 if e.errno != errno.EBADF: log.error(f"Error closing cache fd={cache_fd}: {e}")
            except Exception as e: log.exception(f"Unexpected error during cache fd close for {path}")
        log.info(f"Released fh={fh} (path={path})")
        return 0
    
    def flush(self, path, fh):
        log.debug(f"flush called fh={fh} path={path}")
        h = self._file_handles.get(fh)
        if not h or h['cache_fd'] is None:
            return 0            # completed 文件无 cache_fd
        try:
            os.fsync(h['cache_fd'])
        except OSError as e:
            log.warning(f"fsync failed on fd {h['cache_fd']}: {e}")
        return 0

    def rmdir(self, path):
        # (Implementation mostly same as v2, ensure parent times updated)
        log.info(f"[rmdir START] path='{path}'")
        if path == '/': raise FuseOSError(errno.EPERM)
        parent_path = os.path.dirname(path)
        entry_to_delete = self.db.get_entry_by_path(path)
        if not entry_to_delete: raise FuseOSError(errno.ENOENT)
        if not entry_to_delete['is_dir']: raise FuseOSError(errno.ENOTDIR)
        if not entry_to_delete['is_active']: raise FuseOSError(errno.ENOENT)
        if self.db.get_children(path): raise FuseOSError(errno.ENOTEMPTY)

        cache_path = self._get_real_path(path)
        if cache_path and os.path.exists(cache_path):
            try:
                if os.path.isdir(cache_path):
                    if not os.listdir(cache_path): os.rmdir(cache_path); log.info(f"[rmdir CACHE] Removed cache dir: {cache_path}")
                    else: log.error(f"[rmdir CACHE FAIL] Cache dir {cache_path} not empty!"); raise FuseOSError(errno.ENOTEMPTY)
                else: log.error(f"[rmdir CACHE FAIL] Cache path {cache_path} is not a dir!"); raise FuseOSError(errno.ENOTDIR)
            except OSError as e: log.error(f"[rmdir CACHE FAIL] Error removing cache dir {cache_path}: {e}"); raise FuseOSError(e.errno)

        try:
            if self.db.delete_entry(path): log.info(f"[rmdir DB_DELETE] Removed DB entry: {path}")
            else: log.warning(f"[rmdir DB_DELETE WARN] Failed to remove DB entry: {path}")
        except Exception as e: log.exception(f"[rmdir DB_DELETE FAIL] Error removing DB entry {path}:"); raise FuseOSError(errno.EIO)
        try: self.db.update_entry(parent_path, mtime=time.time(), ctime=time.time())
        except Exception as e: log.exception(f"[rmdir DB_UPDATE FAIL] Failed update parent mtime {parent_path}:")
        log.info(f"[rmdir SUCCESS] Completed rmdir for {path}")
        return 0

    def unlink(self, path):
        # (Implementation mostly same as v2, ensure parent times updated, check active_splitting_tasks)
        log.info(f"[unlink START] path='{path}'")
        if path == '/': raise FuseOSError(errno.EPERM)
        parent_path = os.path.dirname(path)
        entry_to_delete = self.db.get_entry_by_path(path)
        if not entry_to_delete: raise FuseOSError(errno.ENOENT)
        if entry_to_delete['is_dir']: raise FuseOSError(errno.EISDIR)
        if not entry_to_delete['is_active']: raise FuseOSError(errno.ENOENT)

        file_hash = hashlib.md5(path.encode('utf-8')).hexdigest()
        inode = entry_to_delete['inode']

        # Prevent deletion if file is currently being split
        with self.splitting_lock:
             if path in self.active_splitting_tasks:
                 log.warning(f"unlink: File {path} is currently being split. Denying deletion.")
                 raise FuseOSError(errno.EBUSY)

        if not entry_to_delete['is_complete']:
            cache_path = self._get_real_path(path)
            log.debug(f"[unlink CACHE] Removing cache file: {cache_path}")
            if cache_path and os.path.exists(cache_path):
                try: os.unlink(cache_path); log.info(f"[unlink CACHE] Removed cache file: {cache_path}")
                except OSError as e: log.error(f"[unlink CACHE FAIL] Error removing cache file {cache_path}: {e}"); raise FuseOSError(e.errno)
        else:
            if file_hash:
                file_piece_dir = os.path.join(self.piece_dir, file_hash)
                log.debug(f"[unlink PIECE] Removing piece directory: {file_piece_dir}")
                if os.path.exists(file_piece_dir):
                     try: shutil.rmtree(file_piece_dir); log.info(f"[unlink PIECE] Removed piece directory: {file_piece_dir}")
                     except Exception as e: log.error(f"[unlink PIECE FAIL] Error removing piece dir {file_piece_dir}: {e}"); raise FuseOSError(errno.EIO)
            else: log.warning(f"[unlink PIECE WARN] Completed file {path} (inode={inode}) has no file_hash.")

        try:
            if self.db.delete_entry(path): log.info(f"[unlink DB_DELETE] Removed DB entry: {path}")
            else: log.warning(f"[unlink DB_DELETE WARN] Failed to remove DB entry: {path}")
        except Exception as e: log.exception(f"[unlink DB_DELETE FAIL] Error removing DB entry {path}:"); raise FuseOSError(errno.EIO)
        try: self.db.update_entry(parent_path, mtime=time.time(), ctime=time.time())
        except Exception as e: log.exception(f"[unlink DB_UPDATE FAIL] Failed update parent mtime {parent_path}:")
        log.info(f"[unlink SUCCESS] Completed unlink for {path}")
        return 0

    def truncate(self, path, length, fh=None):
        # (Implementation mostly same as v2)
        log.info(f"truncate called for path={path}, length={length}, fh={fh}")
        entry = self.db.get_entry_by_path(path)
        if not entry or not entry['is_active'] or entry['is_dir']: raise FuseOSError(errno.ENOENT)
        if entry['is_complete']: log.warning(f"Attempt truncate completed file {path}. Denied."); raise FuseOSError(errno.EROFS)

        cache_path = self._get_real_path(path)
        if not cache_path: log.error(f"truncate: No cache path for incomplete {path}"); raise FuseOSError(errno.EIO)

        cache_fd_local = -1; opened_here = False
        try:
            if fh is not None and fh in self._file_handles:
                handle_info = self._file_handles[fh]
                cache_fd = handle_info.get('cache_fd')
                if cache_fd is None: raise FuseOSError(errno.EIO)
                handle_flags = handle_info.get('flags', 0)
                if not (handle_flags & (os.O_WRONLY | os.O_RDWR)): raise FuseOSError(errno.EBADF)
                os.ftruncate(cache_fd, length)
            else:
                opened_here = True
                cache_fd_local = os.open(cache_path, os.O_WRONLY)
                os.ftruncate(cache_fd_local, length)

            log.info(f"Truncated cache file {cache_path} to length {length}")
            now = time.time()
            self.db.update_entry(path, size=length, mtime=now, ctime=now)
            log.debug(f"Updated metadata for {path} after truncate: size={length}")
            return 0
        except FileNotFoundError: log.error(f"truncate: Cache file {cache_path} not found"); raise FuseOSError(errno.ENOENT)
        except IsADirectoryError: log.error(f"truncate: Cache path {cache_path} is dir"); raise FuseOSError(errno.EISDIR)
        except OSError as e: log.error(f"Error truncating cache file {cache_path}: {e}"); raise FuseOSError(e.errno)
        finally:
            if opened_here and cache_fd_local != -1:
                try: os.close(cache_fd_local)
                except OSError: pass

    def chmod(self, path, mode):
        """
        Changes the mode (permissions) of a file or directory.
        Files:
        - Prevents changing read-only files back to writable.
        - Triggers splitting when changing from writable to read-only.
        Directories:
        - If removing write bits: Recursively processes children (directory's own permissions remain).
        - Other chmod behavior for directories is unchanged.
        """
        log.info(f"chmod called path={path}, mode={oct(mode)}")
        entry = self.db.get_entry_by_path(path)
        if not entry or not entry["is_active"]:
            raise FuseOSError(errno.ENOENT)

        req_perm = stat.S_IMODE(mode)            # Requested 9 permission bits
        cur_perm = stat.S_IMODE(entry["mode"])   # DB recorded 9 permission bits
        was_writable       = bool(cur_perm & 0o222)
        requesting_writable = bool(req_perm & 0o222)
        requesting_readonly = not requesting_writable

        # ───────────────────── Directory Recursive Logic ──────────────────────
        if entry["is_dir"] and was_writable and requesting_readonly:
            log.info(f"[DIR-SPLIT] '{path}' → readonly, processing children")
            readonly_bits = cur_perm & ~0o222            # Remove write bits but keep r/x
            now = time.time()

            for name in self.db.get_children(path):
                child = path.rstrip("/") + "/" + name if path != "/" else f"/{name}"
                c = self.db.get_entry_by_path(child)
                if not c or not c["is_active"]:
                    continue

                # ---- 1) Remove write bits from physical entity ----
                target = None
                if c["is_dir"] or not c["is_complete"]:
                    target = self._get_real_path(child)
                if target and os.path.exists(target):
                    try:
                        os.chmod(target, readonly_bits)
                    except Exception as e:
                        log.warning(f"[DIR-SPLIT] chmod {child} failed: {e}")
                self.chmod(child,readonly_bits)            
            self.db.update_entry(path, ctime=now)
            return 0
        
        # ───────────────────── Regular File or Other Cases ─────────────────
        # Adding write bits to a directory also goes through this branch;
        # the directory's physical entity is still chmod'd if available.
        physical = None
        if entry["is_dir"] or not entry["is_complete"]:
            physical = self._get_real_path(path)
        if physical and os.path.exists(physical):
            try:
                os.chmod(physical, req_perm)
            except OSError as e:
                raise FuseOSError(e.errno)

        # File: Prevent read-only -> writable; writable -> read-only triggers splitting
        if not entry["is_dir"]:
            if not was_writable and requesting_writable:
                raise FuseOSError(errno.EPERM)
            if was_writable and requesting_readonly:
                # Trigger single file split (using existing implementation)
                try:
                    st = os.stat(physical)
                except Exception:
                    st = None
                self.db.update_entry(
                    path,
                    mode=stat.S_IFMT(entry["mode"]) | req_perm,
                    size=(st.st_size if st else entry["size"]),
                    mtime=(st.st_mtime if st else entry["mtime"]),
                    ctime=time.time(),
                )
                with self.splitting_lock:
                    if path not in self.active_splitting_tasks:
                        fut = self.executor.submit(
                            self._run_splitting_async,
                            path, physical, self.piece_dir,
                            hashlib.md5(path.encode()).hexdigest(),
                            st.st_size if st else entry["size"],
                        )
                        fut.add_done_callback(self._split_task_done_callback)
                        self.active_splitting_tasks[path] = fut
                return 0

        # Other simple chmod: only change mode / ctime
        self.db.update_entry(
            path,
            mode=stat.S_IFMT(entry["mode"]) | req_perm,
            ctime=time.time(),
        )
        return 0

    # --- Other FUSE methods (access, chown, utimens, statfs, rename) ---
    # (Implementations mostly same as v2)
    def access(self, path, amode):
        log.debug(f"access called for path={path}, amode={oct(amode)}")
        entry = self.db.get_entry_by_path(path)
        if not entry or not entry['is_active']: raise FuseOSError(errno.ENOENT)
        # Deny write access check for completed files
        if not entry['is_dir'] and entry['is_complete'] and (amode & os.W_OK):
            log.debug(f"access: Denying W_OK for completed file {path}")
            raise FuseOSError(errno.EACCES)
        # Rely on kernel checks based on getattr mode/uid/gid for other cases
        return 0

    def chown(self, path, uid, gid):
        log.info(f"chown called for path={path}, uid={uid}, gid={gid}")
        entry = self.db.get_entry_by_path(path)
        if not entry or not entry['is_active']: raise FuseOSError(errno.ENOENT)
        target_path = None
        if entry['is_dir']: target_path = self._get_real_path(path)
        elif not entry['is_complete']: target_path = self._get_real_path(path)
        if target_path and os.path.exists(target_path):
             try: os.chown(target_path, uid, gid)
             except OSError as e: log.error(f"chown: Error on {target_path}: {e}"); raise FuseOSError(e.errno)
        else: log.warning(f"chown: Target path {target_path} missing for {path}.")
        try: self.db.update_entry(path, uid=uid, gid=gid, ctime=time.time())
        except Exception as e: log.exception(f"chown: DB update error for {path}:"); raise FuseOSError(errno.EIO)
        return 0

    def utimens(self, path, times=None):
         log.debug(f"utimens called for path={path}, times={times}")
         entry = self.db.get_entry_by_path(path)
         if not entry or not entry['is_active']: raise FuseOSError(errno.ENOENT)
         now = time.time(); atime_ns, mtime_ns = times if times else (now, now)
         target_path = None
         if entry['is_dir']: target_path = self._get_real_path(path)
         elif not entry['is_complete']: target_path = self._get_real_path(path)
         if target_path and os.path.exists(target_path):
              try: os.utime(target_path, (atime_ns, mtime_ns))
              except OSError as e: log.error(f"utimens: Error on {target_path}: {e}"); raise FuseOSError(e.errno)
         else: log.warning(f"utimens: Target path {target_path} missing for {path}.")
         try: self.db.update_entry(path, atime=atime_ns, mtime=mtime_ns, ctime=now)
         except Exception as e: log.exception(f"utimens: DB update error for {path}:"); raise FuseOSError(errno.EIO)
         return 0

    def statfs(self, path):
        # (Implementation same as v2)
        try: stv = os.statvfs(self.cache_dir)
        except OSError: stv = None
        block_size = stv.f_frsize if stv else 4096
        total_blocks = stv.f_blocks if stv else 1024*1024
        free_blocks = stv.f_bavail if stv else 512*1024
        return dict(f_bsize=block_size, f_frsize=block_size, f_blocks=total_blocks,
                    f_bfree=free_blocks, f_bavail=free_blocks, f_files=10000,
                    f_ffree=5000, f_favail=5000, f_fsid=0, f_flag=0, f_namemax=255)

    def rename(self, old, new):
        log.info(f"rename called: '{old}' → '{new}'")

        # --- basic validation -------------------------------------------------
        if old == '/' or new == '/':
            raise FuseOSError(errno.EINVAL)

        old_entry = self.db.get_entry_by_path(old)
        if not old_entry or not old_entry['is_active']:
            raise FuseOSError(errno.ENOENT)

        # 1) disallow directory rename
        if old_entry['is_dir']:
            raise FuseOSError(errno.EISDIR)

        # 2) disallow renaming files that are already read‑only
        if (old_entry['mode'] & 0o222) == 0:
            raise FuseOSError(errno.EPERM)

        new_entry = self.db.get_entry_by_path(new)
        new_parent = os.path.dirname(new)
        new_parent_entry = self.db.get_entry_by_path(new_parent)
        if not new_parent_entry or not new_parent_entry['is_dir'] or not new_parent_entry['is_active']:
            raise FuseOSError(errno.ENOENT)

        # Overwrite existing *file* target if present (directories are forbidden)
        if new_entry and new_entry['is_active']:
            if new_entry['is_dir']:
                raise FuseOSError(errno.EISDIR)
            self.unlink(new)

        # --- move physical file when necessary --------------------------------
        if not old_entry['is_complete']:
            old_real = self._get_real_path(old)
            new_real = self._get_real_path(new)
            try:
                os.makedirs(os.path.dirname(new_real), exist_ok=True)
                os.rename(old_real, new_real)
                log.debug(f"Renamed cache file {old_real} → {new_real}")
            except OSError as e:
                log.error(f"rename: failed to move cache file '{old_real}' → '{new_real}': {e}")
                raise FuseOSError(e.errno)

        # --- update metadata ---------------------------------------------------
        now = time.time()
        if not self.db.rename_path(old, new):
            raise FuseOSError(errno.EIO)

        # touch parent directories
        self.db.update_entry(os.path.dirname(old), mtime=now, ctime=now)
        if os.path.dirname(old) != new_parent:
            self.db.update_entry(new_parent, mtime=now, ctime=now)

        return 0
