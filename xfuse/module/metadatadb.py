from contextlib import contextmanager
import os
import sqlite3
import stat
import threading
import time
from xfuse.config import log

# --- Metadata Database Management ---
class MetadataDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self._lock = threading.RLock() # Use re-entrant lock
        self._init_db()

    @contextmanager
    def _cursor(self):
        """Provides a transactional and locked database cursor context manager."""
        with self._lock:
            # Consider increasing timeout if experiencing frequent busy errors
            conn = sqlite3.connect(self.db_path, timeout=15)
            conn.row_factory = sqlite3.Row
            try:
                # WAL mode is generally good for concurrency
                conn.execute("PRAGMA journal_mode=WAL;")
            except Exception as e:
                log.warning(f"Could not set WAL mode: {e}")
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                log.exception("Database transaction rolled back due to error:")
                raise
            finally:
                cursor.close()
                conn.close()

    def _init_db(self):
        """Initializes the database table structure."""
        log.info(f"Initializing database: {self.db_path}")
        with self._cursor() as cur:
            # Metadata table schema (includes fields for splitting)
            cur.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    inode INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_path TEXT NOT NULL,
                    name TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE,
                    is_dir INTEGER NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    is_complete INTEGER DEFAULT 0, -- 0: Incomplete, 1: Complete & Split
                    size INTEGER DEFAULT 0,
                    mode INTEGER DEFAULT 0,
                    uid INTEGER DEFAULT 0,
                    gid INTEGER DEFAULT 0,
                    mtime REAL DEFAULT 0,
                    atime REAL DEFAULT 0,
                    ctime REAL DEFAULT 0,
                    torrent_path TEXT,
                    file_hash TEXT,      -- File hash (for piece directory)
                    piece_count INTEGER, -- Number of pieces
                    piece_size INTEGER   -- Size of each piece
                )
            ''')
            # Create root directory if it doesn't exist
            cur.execute("SELECT inode FROM metadata WHERE path = '/'")
            if not cur.fetchone():
                 now = time.time()
                 cur.execute('''
                    INSERT INTO metadata (parent_path, name, path, is_dir, is_active, mode, uid, gid, mtime, atime, ctime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ''', ('', '/', '/', 1, 1, stat.S_IFDIR | 0o755, os.getuid(), os.getgid(), now, now, now))

            # Create indices for faster lookups
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_path ON metadata (path)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_parent_path ON metadata (parent_path)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_active_complete ON metadata (is_active, is_complete, is_dir)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_file_hash ON metadata (file_hash)")

    def get_entry_by_path(self, path):
        """Retrieves a metadata entry by its full path."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM metadata WHERE path = ?", (path,))
            return cur.fetchone()

    def get_children(self, parent_path):
        """Gets the names of all active children under a given parent path."""
        with self._cursor() as cur:
            # Ensure parent path format for query
            if parent_path != '/' and not parent_path.endswith('/'):
                parent_path += '/'
            cur.execute("SELECT name FROM metadata WHERE parent_path = ? AND is_active = 1", (parent_path,))
            return [row['name'] for row in cur.fetchall()]

    def add_entry(self, parent_path, name, path, is_dir, mode, size=0, torrent_path=None, file_hash=None):
        """Adds a new metadata entry (includes file_hash)."""
        now = time.time()
        uid = os.getuid()
        gid = os.getgid()
        with self._cursor() as cur:
            cur.execute('''
                INSERT INTO metadata (parent_path, name, path, is_dir, is_active, is_complete, size, mode, uid, gid, mtime, atime, ctime, torrent_path, file_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (parent_path, name, path, 1 if is_dir else 0, 1, 0, size, mode, uid, gid, now, now, now, torrent_path, file_hash))
            return cur.lastrowid # Return the new inode

    def update_status(self, path, is_active=None, is_complete=None):
        """Updates the status flags (is_active, is_complete) for an entry."""
        updates = []
        params = []
        if is_active is not None: updates.append("is_active = ?"); params.append(1 if is_active else 0)
        if is_complete is not None: updates.append("is_complete = ?"); params.append(1 if is_complete else 0)

        if not updates: return False
        params.append(time.time()); params.append(path) # Update ctime
        with self._cursor() as cur:
            cur.execute(f"UPDATE metadata SET {', '.join(updates)}, ctime = ? WHERE path = ?", tuple(params))
            return cur.rowcount > 0

    def update_entry(self, path, size=None, mode=None, uid=None, gid=None, mtime=None, atime=None, ctime=None, file_hash=None):
        """Updates various attributes of a metadata entry."""
        updates = []; params = []; now = time.time()
        if size is not None: updates.append("size = ?"); params.append(size)
        if mode is not None: updates.append("mode = ?"); params.append(mode)
        if uid is not None: updates.append("uid = ?"); params.append(uid)
        if gid is not None: updates.append("gid = ?"); params.append(gid)
        if mtime is not None: updates.append("mtime = ?"); params.append(mtime)
        # Update mtime if size or mode changes and mtime not specified
        elif (size is not None or mode is not None) and mtime is None: updates.append("mtime = ?"); params.append(now)
        if atime is not None: updates.append("atime = ?"); params.append(atime)
        if file_hash is not None: updates.append("file_hash = ?"); params.append(file_hash)
        # Always update ctime on metadata change
        updates.append("ctime = ?"); params.append(ctime if ctime is not None else now)

        if not updates: return False # Avoid empty update query
        params.append(path)
        with self._cursor() as cur:
            cur.execute(f"UPDATE metadata SET {', '.join(updates)} WHERE path = ?", tuple(params))
            return cur.rowcount > 0

    def update_piece_info(self, path, piece_count, piece_size, is_complete=True):
        """Updates piece info and marks the file as complete after successful splitting."""
        now = time.time()
        with self._cursor() as cur:
            # Also update mtime to reflect the "completion" time
            cur.execute('''
                UPDATE metadata
                SET piece_count = ?, piece_size = ?, is_complete = ?, ctime = ?, mtime = ?
                WHERE path = ?
            ''', (piece_count, piece_size, 1 if is_complete else 0, now, now, path))
            return cur.rowcount > 0
        
    def rename_path(self, old_path: str, new_path: str) -> bool:
        new_name       = os.path.basename(new_path)
        parent         = os.path.dirname(new_path) or '/'
        parent_for_db  = parent if parent == '/' else parent.rstrip('/') + '/'
        now            = time.time()
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE metadata
                   SET path        = ?,
                       name        = ?,
                       parent_path = ?,
                       ctime       = ?
                 WHERE path        = ?
                """,
                (new_path, new_name, parent_for_db, now, old_path),
            )
            return cur.rowcount > 0

    def delete_entry(self, path):
        """Deletes a metadata entry by its path."""
        with self._cursor() as cur:
            cur.execute("DELETE FROM metadata WHERE path = ?", (path,))
            return cur.rowcount > 0

    def find_active_incomplete_files(self):
        """Finds active files marked as incomplete in the DB (for Scanner)."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT path, name, size, torrent_path, file_hash, inode, mode
                FROM metadata
                WHERE is_active = 1 AND is_complete = 0 AND is_dir = 0
            """)
            return cur.fetchall()

    def set_inactive_recursive(self, path):
        """Recursively marks a directory and its contents as inactive."""
        # (Implementation remains the same as previous versions)
        paths_to_process = [path]; processed_paths = set()
        with self._cursor() as cur:
            while paths_to_process:
                current_path = paths_to_process.pop(0)
                if current_path in processed_paths: continue
                cur.execute("UPDATE metadata SET is_active = 0, ctime = ? WHERE path = ?", (time.time(), current_path))
                processed_paths.add(current_path); log.debug(f"Marked inactive: {current_path}")
                parent_path_query = current_path
                if parent_path_query != '/' and not parent_path_query.endswith('/'): parent_path_query += '/'
                cur.execute("SELECT path FROM metadata WHERE parent_path = ?", (parent_path_query,))
                children = cur.fetchall()
                for child in children:
                    child_path = child['path']
                    if child_path not in processed_paths: paths_to_process.append(child_path)
            log.info(f"Recursively marked path {path} and children as inactive.")

    def find_readonly_incomplete_files(self):
            """Return all *active*, still-incomplete regular files that are
            already read-only (write bits cleared)."""
            write_mask = 0o222          # decimal 146 – but pass as parameter
            with self._cursor() as cur:
                cur.execute("""
                    SELECT path, size, file_hash, torrent_path
                    FROM metadata
                    WHERE is_active   = 1
                    AND is_complete = 0
                    AND is_dir      = 0
                    AND (mode & ?)  = 0      -- no write bits
                """, (write_mask,))
                return cur.fetchall()