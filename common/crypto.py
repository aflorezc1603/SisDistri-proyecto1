import hashlib
import os
from typing import Tuple
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from common.config import AES_ENCRYPTION_KEY

NONCE_LEN = 16
TAG_LEN = 16

def calculate_sha256(data: bytes) -> str:
    """Computes the SHA-256 cryptographic hash of data (RNF3 - Consistencia e integridad)."""
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def encrypt_block(data: bytes, key: bytes = AES_ENCRYPTION_KEY) -> bytes:
    """
    Encrypts data chunk using AES-256-GCM (RNF6 - Seguridad en reposo).
    Returns binary format: 16-byte Nonce + 16-byte Auth Tag + Ciphertext.
    """
    if len(key) != 32:
        raise ValueError(f"AES key must be exactly 32 bytes (256 bits), got {len(key)}")
    nonce = get_random_bytes(NONCE_LEN)
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    ciphertext, tag = cipher.encrypt_and_digest(data)
    return nonce + tag + ciphertext

def decrypt_block(encrypted_data: bytes, key: bytes = AES_ENCRYPTION_KEY) -> bytes:
    """
    Decrypts and verifies an AES-256-GCM block chunk.
    Raises ValueError if authentication tag does not match (tampering detected).
    """
    if len(key) != 32:
        raise ValueError(f"AES key must be exactly 32 bytes (256 bits), got {len(key)}")
    header_len = NONCE_LEN + TAG_LEN
    if len(encrypted_data) < header_len:
        raise ValueError("Encrypted data is too short to contain Nonce and Auth Tag")

    nonce = encrypted_data[:NONCE_LEN]
    tag = encrypted_data[NONCE_LEN:header_len]
    ciphertext = encrypted_data[header_len:]

    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    plaintext = cipher.decrypt_and_verify(ciphertext, tag)
    return plaintext
