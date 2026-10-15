import os
import shutil
from typing import Tuple, List, Optional
from common.crypto import encrypt_block, decrypt_block, calculate_sha256
from common.config import DATA_DIR
from common.logger import setup_logger

logger = setup_logger("DataNodeStorage")

class ChunkStorageManager:
    """
    Manages local chunk storage on disk with AES-256-GCM encryption at rest (RNF6)
    and SHA-256 checksum verification (RNF3).
    """
    def __init__(self, base_dir: str = DATA_DIR):
        self.base_dir = os.path.abspath(base_dir)
        self.chunks_dir = os.path.join(self.base_dir, "chunks")
        os.makedirs(self.chunks_dir, exist_ok=True)
        logger.info(f"Chunk storage initialized at: {self.chunks_dir}")

    def _chunk_file_path(self, chunk_id: str) -> str:
        # Sanitize chunk_id to avoid path traversal
        clean_id = os.path.basename(chunk_id)
        return os.path.join(self.chunks_dir, f"{clean_id}.enc")

    def save_chunk(self, chunk_id: str, plaintext_data: bytes, expected_sha256: Optional[str] = None) -> Tuple[int, str]:
        """
        Encrypts and stores a chunk on disk with AES-256-GCM.
        Validates SHA-256 checksum before persisting.
        """
        actual_sha256 = calculate_sha256(plaintext_data)
        if expected_sha256 and actual_sha256.lower() != expected_sha256.lower():
            raise ValueError(f"Integrity check failed: expected SHA-256 {expected_sha256}, got {actual_sha256}")

        # Encrypt with AES-256-GCM (RNF6)
        encrypted_data = encrypt_block(plaintext_data)

        dest_path = self._chunk_file_path(chunk_id)
        temp_path = dest_path + ".tmp"

        with open(temp_path, "wb") as f:
            f.write(encrypted_data)

        # Atomic rename to prevent corrupt chunks during unexpected crash
        os.replace(temp_path, dest_path)
        logger.debug(f"Saved chunk {chunk_id}: {len(plaintext_data)} bytes plaintext, {len(encrypted_data)} bytes encrypted")
        return len(plaintext_data), actual_sha256

    def read_chunk(self, chunk_id: str) -> Tuple[bytes, str]:
        """
        Reads, decrypts and validates a chunk from disk.
        """
        dest_path = self._chunk_file_path(chunk_id)
        if not os.path.exists(dest_path):
            raise FileNotFoundError(f"Chunk {chunk_id} not found on this node")

        with open(dest_path, "rb") as f:
            encrypted_data = f.read()

        # Decrypt with AES-256-GCM (verifies tag internally)
        plaintext_data = decrypt_block(encrypted_data)
        sha256_hash = calculate_sha256(plaintext_data)
        return plaintext_data, sha256_hash

    def delete_chunk(self, chunk_id: str) -> bool:
        dest_path = self._chunk_file_path(chunk_id)
        if os.path.exists(dest_path):
            os.remove(dest_path)
            logger.info(f"Deleted chunk {chunk_id}")
            return True
        return False

    def list_chunks(self) -> List[str]:
        chunks = []
        if os.path.exists(self.chunks_dir):
            for fname in os.listdir(self.chunks_dir):
                if fname.endswith(".enc"):
                    chunks.append(fname[:-4])
        return chunks

    def get_storage_stats(self) -> Tuple[int, int]:
        """Returns (total_space_bytes, available_space_bytes) for the storage mount."""
        try:
            usage = shutil.disk_usage(self.base_dir)
            return usage.total, usage.free
        except Exception:
            return 10 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024
