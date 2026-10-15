from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any

# ==========================================
# Authentication & User Models (RNF6)
# ==========================================

class UserRegisterRequest(BaseModel):
    username: str
    password: str
    role: str = "user"  # "admin" or "user"

class UserLoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    username: str
    role: str

class UserInfo(BaseModel):
    username: str
    role: str
    created_at: float

# ==========================================
# Node Registration & Heartbeat (ControlNode <-> DataNode)
# ==========================================

class NodeRegistrationRequest(BaseModel):
    node_id: str
    host: str
    port: int
    url: str
    total_space: int = 10 * 1024 * 1024 * 1024  # 10 GB default
    available_space: int = 10 * 1024 * 1024 * 1024

class HeartbeatRequest(BaseModel):
    node_id: str
    available_space: int
    active_chunks: List[str] = Field(default_factory=list)

class HeartbeatResponse(BaseModel):
    status: str = "ok"
    replicate_commands: List[Dict[str, Any]] = Field(default_factory=list)
    delete_commands: List[str] = Field(default_factory=list)

class NodeInfo(BaseModel):
    node_id: str
    host: str
    port: int
    url: str
    total_space: int
    available_space: int
    last_heartbeat: float
    is_alive: bool = True

# ==========================================
# File System Hierarchy & Metadata (RF1 & RF3)
# ==========================================

class ChunkMetadata(BaseModel):
    chunk_id: str
    chunk_index: int
    size_bytes: int
    sha256: str
    locations: List[str] = Field(default_factory=list)  # list of node_ids

class FileInfo(BaseModel):
    path: str
    name: str
    is_directory: bool
    size_bytes: int = 0
    owner: str = "system"
    permissions: str = "rw-r--r--"
    created_at: float
    modified_at: float
    chunks: List[ChunkMetadata] = Field(default_factory=list)
    is_locked: bool = False
    locked_by: Optional[str] = None
    lock_type: Optional[str] = None

class DirectoryListing(BaseModel):
    path: str
    items: List[FileInfo]

class PathRequest(BaseModel):
    path: str

class RmRequest(BaseModel):
    path: str
    recursive: bool = False

# ==========================================
# Transfer Coordination (RF2 & RNF4)
# ==========================================

class ChunkPlan(BaseModel):
    chunk_id: str
    chunk_index: int
    expected_size: int
    primary_node: NodeInfo
    replica_nodes: List[NodeInfo] = Field(default_factory=list)

class InitUploadRequest(BaseModel):
    path: str
    file_size: int
    chunk_size: int = 1 * 1024 * 1024
    replication_factor: int = 2

class InitUploadResponse(BaseModel):
    path: str
    upload_id: str
    chunk_size: int
    total_chunks: int
    chunks: List[ChunkPlan]

class CommitChunkInfo(BaseModel):
    chunk_id: str
    chunk_index: int
    size_bytes: int
    sha256: str
    stored_nodes: List[str]

class CommitUploadRequest(BaseModel):
    path: str
    upload_id: str
    total_size: int
    chunks: List[CommitChunkInfo]

class ChunkDownloadLocation(BaseModel):
    chunk_id: str
    chunk_index: int
    size_bytes: int
    sha256: str
    locations: List[NodeInfo]

class DownloadFileInfoResponse(BaseModel):
    path: str
    size_bytes: int
    total_chunks: int
    chunks: List[ChunkDownloadLocation]

# ==========================================
# File Access Primitives & Locking (RF3)
# ==========================================

class LockAcquireRequest(BaseModel):
    path: str
    lock_type: str = "exclusive"  # "shared" or "exclusive"

class LockReleaseRequest(BaseModel):
    path: str

class LockStatusResponse(BaseModel):
    path: str
    is_locked: bool
    locked_by: Optional[str] = None
    lock_type: Optional[str] = None

# ==========================================
# High Availability State Sync (RNF2)
# ==========================================

class StateSyncSnapshot(BaseModel):
    term: int
    timestamp: float
    users: Dict[str, Any]
    fs_tree: Dict[str, Any]
    nodes: Dict[str, Any]
    locks: Dict[str, Any]

class StatusResponse(BaseModel):
    status: str
    message: str
    detail: Optional[Any] = None
