import logging
import os

SCRIPT_DIR = os.getcwd()
TORRENT_DIR = os.path.join(SCRIPT_DIR, 'torrent')
CACHE_DIR = os.path.join(SCRIPT_DIR, 'cache')
PIECE_DIR = os.path.join(SCRIPT_DIR, 'piece')
METADATA_DB_PATH = os.path.join(SCRIPT_DIR, 'metadatadb.sqlite')
DEFAULT_MNT_DIR = os.path.join(SCRIPT_DIR, 'mnt')

# Piece constants
PIECE_SIZE = 8 * 1024 * 1024
MAX_PIECES_LARGE_FILE = 500
DEFAULT_DURATION_PER_PIECE = 60 # seconds (Placeholder for time-based splitting)

# --- Logging Configuration ---
log = logging.getLogger(__name__)
# Adjust level as needed, e.g., logging.INFO for less verbose output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')