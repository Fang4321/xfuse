import hashlib
import math
import os
import shutil
import bencodepy
from xfuse.config import MAX_PIECES_LARGE_FILE, PIECE_SIZE, log

# --- Torrent Related Placeholders ---
def parse_torrent_file(torrent_path):
    try:
        data = bencodepy.decode_from_file(torrent_path)
        info = data[b'info']
        name = info[b'name'].decode('utf-8')
        # 文件列表
        files = []
        if b'files' in info:
            for f in info[b'files']:
                rel = b'/'.join(f[b'path']).decode('utf-8')
                length = f[b'length']
                files.append({'path': rel, 'length': length})
        else:
            length = info[b'length']
            files.append({'path': name, 'length': length})
        total_size = sum(f['length'] for f in files)
        # 分片信息
        piece_length = info[b'piece length']
        raw_pieces = info[b'pieces']
        pieces = [raw_pieces[i*20:(i+1)*20] for i in range(len(raw_pieces)//20)]
        # info_hash
        raw_info = bencodepy.encode(info)
        info_hash = hashlib.sha1(raw_info).hexdigest()
        return {
            'name': name,
            'files': files,
            'total_size': total_size,
            'piece_length': piece_length,
            'pieces': pieces,
            'info_hash': info_hash,
            'is_multi': b'files' in info
        }
    except Exception as e:
        log.error(f"parse_torrent_file error: {e}")
        return None


def verify_file_completeness(cache_path, expected_size, torrent_info):
    """
    Placeholder for verifying file completeness using torrent piece hashes.
    !!! CRITICAL: Replace with actual piece verification logic !!!
    """
    log.debug(f"Verifying completeness for {cache_path} (expected size: {expected_size}) using torrent {torrent_info.get('name', 'N/A')}")
    if not os.path.exists(cache_path):
        log.warning(f"Cache file {cache_path} does not exist for verification.")
        return False
    actual_size = os.path.getsize(cache_path)
    if actual_size != expected_size:
        log.warning(f"Size mismatch for {cache_path}: expected {expected_size}, got {actual_size}. Verification failed.")
        return False # Size mismatch means incomplete/corrupt

    # !!! Add loop here to read file chunks and compare hashes with torrent_info !!!
    # Example:
    # piece_length = torrent_info.get('piece_length')
    # pieces_hashes = torrent_info.get('pieces') # Assuming this is a list/bytes of hashes
    # num_pieces = torrent_info.get('num_pieces')
    # with open(cache_path, 'rb') as f:
    #     for i in range(num_pieces):
    #         chunk = f.read(piece_length) # Adjust for last piece size
    #         if not check_hash(chunk, pieces_hashes[i]):
    #              log.warning(f"Piece {i} hash mismatch for {cache_path}")
    #              return False

    log.info(f"Completeness check PASSED (placeholder) for {cache_path}")
    return True


# --- File Splitting Logic ---
def calculate_piece_config(file_size):
    """Calculates piece count and size based on file size rules."""
    log.debug(f"Calculating pieces based on size: {file_size} bytes")
    if file_size == 0:
         return 1, 0 # Special case for empty file
    elif file_size < PIECE_SIZE * MAX_PIECES_LARGE_FILE:
        piece_size = PIECE_SIZE
        num_pieces = math.ceil(file_size / piece_size)
        log.info(f"Size-based (<4GB): {num_pieces} pieces, {piece_size} bytes/piece")
    else: # >= 4GB
        num_pieces = MAX_PIECES_LARGE_FILE
        piece_size = math.ceil(file_size / num_pieces)
        log.info(f"Size-based (>=4GB): {num_pieces} pieces (max), ~{piece_size} bytes/piece")

    # Ensure sane values
    if piece_size <= 0: piece_size = 1
    if num_pieces <= 0: num_pieces = 1
    return num_pieces, piece_size

def split_file_into_pieces(cache_path, piece_dir, file_hash, file_size):
    """
    Splits the cache file into pieces according to calculated config.
    Returns (piece_count, piece_size) on success, None on failure.
    """
    log.info(f"Starting to split file: {cache_path} (size: {file_size}) using hash: {file_hash}")

    if not file_hash: log.error("File hash missing for splitting."); return None
    if not os.path.exists(cache_path): log.error(f"Cache file {cache_path} not found."); return None

    # Verify actual size matches expected size before splitting
    actual_cache_size = os.path.getsize(cache_path)
    if actual_cache_size != file_size:
        log.error(f"Cache file size mismatch before split: expected {file_size}, got {actual_cache_size}. Aborting split.")
        # This prevents splitting potentially incomplete files triggered by premature chmod
        return None

    # Calculate splitting configuration
    num_pieces, piece_size = calculate_piece_config(file_size)
    if num_pieces == 1 and piece_size == 0 and file_size > 0: # Sanity check config
        log.error(f"Invalid piece config for non-empty file {cache_path}. Aborting.")
        return None

    file_piece_dir = os.path.join(piece_dir, file_hash)
    log.debug(f"Target piece directory: {file_piece_dir}")

    try:
        os.makedirs(file_piece_dir, exist_ok=True)
        bytes_processed = 0
        actual_pieces_created = 0
        with open(cache_path, 'rb') as f_in:
            for i in range(num_pieces):
                piece_path = os.path.join(file_piece_dir, str(i))
                bytes_to_read = min(piece_size, file_size - bytes_processed)

                if bytes_to_read < 0: # Should not happen
                     log.error(f"Negative bytes_to_read ({bytes_to_read}) for piece {i}. Aborting.")
                     raise IOError("Negative read size calculated during split")

                if bytes_to_read == 0:
                     if bytes_processed == file_size: # Normal end of file
                         log.debug(f"Reached end of file at piece {i}. Processed: {bytes_processed}")
                         break
                     elif file_size == 0 and i == 0: # Handle zero-byte file
                         log.debug("Creating empty piece 0 for zero-byte file.")
                         with open(piece_path, 'wb') as f_out: pass
                         bytes_processed = 0
                         actual_pieces_created = 1
                         break # Only one piece for empty file
                     else: # Unexpected zero read size
                         log.warning(f"Unexpected zero bytes_to_read at piece {i} while {bytes_processed}/{file_size} processed. Stopping.")
                         break

                # Read chunk from cache and write to piece file
                chunk = f_in.read(bytes_to_read)
                if len(chunk) != bytes_to_read:
                    log.error(f"Read {len(chunk)} bytes, expected {bytes_to_read} for piece {i}. File truncated? Aborting.")
                    raise IOError(f"Short read during split for piece {i}")

                with open(piece_path, 'wb') as f_out:
                    f_out.write(chunk)

                bytes_processed += len(chunk)
                actual_pieces_created += 1
                log.debug(f"Created piece: {piece_path} ({len(chunk)} bytes)")

        # Final check: ensure all bytes were processed
        if bytes_processed != file_size:
             log.error(f"Splitting finished, but bytes processed ({bytes_processed}) != file size ({file_size}).")
             # Clean up potentially incomplete pieces
             raise IOError("Byte count mismatch after splitting")

        log.info(f"Successfully created {actual_pieces_created} pieces for hash {file_hash} in {file_piece_dir}")
        # Return the *calculated* piece size, but the *actual* number created
        return actual_pieces_created, piece_size

    except Exception as e:
        log.exception(f"Error splitting file {cache_path} for hash {file_hash}:")
        # Clean up piece directory on error
        if os.path.exists(file_piece_dir):
            try:
                shutil.rmtree(file_piece_dir)
                log.info(f"Cleaned up partial piece directory: {file_piece_dir}")
            except Exception as clean_e:
                log.error(f"Error cleaning up piece directory {file_piece_dir}: {clean_e}")
        return None


# --- Piece Reading ---
def read_piece(piece_dir, file_hash, piece_index, offset, size):
    """Reads data from a specific piece file."""
    if not file_hash: log.error("read_piece: file_hash missing."); return b''
    piece_path = os.path.join(piece_dir, file_hash, str(piece_index))
    # log.debug(f"Reading piece: path={piece_path}, offset={offset}, size={size}") # Can be very verbose
    try:
        if not os.path.exists(piece_path): log.warning(f"Piece file not found: {piece_path}"); return b''
        piece_file_size = os.path.getsize(piece_path)
        if offset >= piece_file_size: return b'' # Offset is beyond the piece
        effective_size = min(size, piece_file_size - offset)
        if effective_size <= 0: return b''

        with open(piece_path, 'rb') as f:
            f.seek(offset)
            data = f.read(effective_size)
            # log.debug(f"Read {len(data)} bytes from piece {piece_index}")
            return data
    except OSError as e: log.error(f"Error reading piece file {piece_path}: {e}"); return b''
    except Exception as e: log.exception(f"Unexpected error reading piece {piece_path}"); return b''

