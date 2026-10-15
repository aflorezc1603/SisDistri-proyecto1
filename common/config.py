import os

# Configuration settings for DFSha cluster

# Chunk & Partitioning Settings (RNF4)
DEFAULT_CHUNK_SIZE = int(os.getenv("DFSHA_CHUNK_SIZE", str(1 * 1024 * 1024)))  # 1 MB default
DEFAULT_REPLICATION_FACTOR = int(os.getenv("DFSHA_REPLICATION_FACTOR", "2"))  # 2 replicas by default

# Heartbeat & Health Monitoring (RNF2 - Alta Disponibilidad)
HEARTBEAT_INTERVAL_SEC = int(os.getenv("DFSHA_HEARTBEAT_INTERVAL", "5"))
HEARTBEAT_TIMEOUT_SEC = int(os.getenv("DFSHA_HEARTBEAT_TIMEOUT", "15"))

# Security & Cryptography (RNF6)
SECRET_KEY = os.getenv("DFSHA_SECRET_KEY", "dfsha-super-secure-jwt-signing-secret-key-2026")
CLUSTER_SECRET = os.getenv("DFSHA_CLUSTER_SECRET", "dfsha-internal-s2s-cluster-secret-token-2026")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours
CLUSTER_AUTH_HEADER = "X-DFSha-Cluster-Token"

# Master key for AES-256-GCM encryption at rest (32 bytes hex)
# Default is 32 bytes ASCII (256 bits)
DEFAULT_AES_KEY = "0123456789abcdef0123456789abcdef"
AES_ENCRYPTION_KEY = os.getenv("DFSHA_AES_KEY", DEFAULT_AES_KEY).encode("utf-8")[:32]

# Storage Paths
DATA_DIR = os.getenv("DFSHA_DATA_DIR", "./data")
METADATA_DIR = os.getenv("DFSHA_METADATA_DIR", "./metadata")
