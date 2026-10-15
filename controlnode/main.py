import asyncio
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
import httpx
from fastapi import FastAPI, HTTPException, Depends, Security, Header, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse

from common.config import (
    CLUSTER_SECRET,
    CLUSTER_AUTH_HEADER,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_REPLICATION_FACTOR
)
from common.logger import setup_logger
from common.models import (
    UserRegisterRequest,
    UserLoginRequest,
    TokenResponse,
    UserInfo,
    NodeRegistrationRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    NodeInfo,
    FileInfo,
    DirectoryListing,
    PathRequest,
    RmRequest,
    InitUploadRequest,
    InitUploadResponse,
    CommitUploadRequest,
    DownloadFileInfoResponse,
    ChunkDownloadLocation,
    LockAcquireRequest,
    LockReleaseRequest,
    LockStatusResponse,
    StatusResponse
)
from common.security import (
    hash_password,
    verify_password,
    create_access_token,
    decode_access_token,
    verify_cluster_token
)
from controlnode.metadata_store import MetadataStore
from controlnode.ha_manager import HAManager

HOST = os.getenv("DFSHA_HOST", "0.0.0.0")
PORT = int(os.getenv("DFSHA_PORT", "8000"))
STANDBY_URL = os.getenv("DFSHA_STANDBY_URL", "")

logger = setup_logger("ControlNode")
store = MetadataStore()
ha_mgr = HAManager(store)

security = HTTPBearer(auto_error=False)

async def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Security(security)) -> str:
    """Validates JWT access token and returns username (RNF6)."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication credentials"
        )
    payload = decode_access_token(credentials.credentials)
    if not payload or "sub" not in payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication token"
        )
    username = payload["sub"]
    if not store.get_user(username):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found in metadata store"
        )
    return username

def verify_internal_auth(token: Optional[str] = Header(None, alias=CLUSTER_AUTH_HEADER)):
    """Verifies S2S cluster authentication header for internal operations."""
    if not verify_cluster_token(token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Unauthorized internal cluster request"
        )

@asynccontextmanager
async def lifespan(app: FastAPI):
    await ha_mgr.start()
    yield
    await ha_mgr.stop()

app = FastAPI(title="DFSha ControlNode (Master)", lifespan=lifespan)

# Helper to sync write operations to standby
async def sync_to_standby_if_needed():
    if STANDBY_URL and ha_mgr.is_primary:
        asyncio.create_task(ha_mgr.sync_mutation_to_standby(STANDBY_URL))

# ==========================================
# Health & HA Endpoints (RNF2)
# ==========================================

@app.get("/health")
async def health_check():
    alive_nodes = store.get_alive_nodes()
    return {
        "status": "healthy",
        "role": ha_mgr.role,
        "is_primary": ha_mgr.is_primary,
        "alive_datanodes": len(alive_nodes),
        "total_files": len(store.fs_tree)
    }

@app.get("/ha/snapshot", dependencies=[Depends(verify_internal_auth)])
async def get_ha_snapshot():
    """Returns complete state snapshot for standby synchronization."""
    return store.get_state_snapshot()

@app.post("/ha/sync", dependencies=[Depends(verify_internal_auth)])
async def apply_ha_sync(snapshot: dict):
    """Applies state snapshot sent from primary."""
    store.apply_state_snapshot(snapshot)
    return {"status": "synchronized"}

@app.post("/ha/promote", dependencies=[Depends(verify_internal_auth)])
async def manual_promote():
    """Manually promotes this node to primary."""
    ha_mgr.promote_to_primary()
    return {"status": "promoted", "role": ha_mgr.role}

# ==========================================
# Authentication & User Management (RNF6)
# ==========================================

@app.post("/auth/register", response_model=StatusResponse)
async def register_user(req: UserRegisterRequest):
    if not ha_mgr.is_primary:
        raise HTTPException(status_code=503, detail="Standby node is read-only")

    pw_hash = hash_password(req.password)
    success = store.add_user(req.username, pw_hash, req.role)
    if not success:
        raise HTTPException(status_code=400, detail="Username already exists")

    await sync_to_standby_if_needed()
    return StatusResponse(status="success", message=f"User '{req.username}' created successfully")

@app.post("/auth/login", response_model=TokenResponse)
async def login_user(req: UserLoginRequest):
    user = store.get_user(req.username)
    if not user or not verify_password(req.password, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = create_access_token({"sub": req.username, "role": user.get("role", "user")})
    return TokenResponse(
        access_token=token,
        username=req.username,
        role=user.get("role", "user")
    )

@app.get("/auth/me", response_model=UserInfo)
async def get_me(username: str = Depends(get_current_user)):
    user = store.get_user(username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserInfo(
        username=user["username"],
        role=user.get("role", "user"),
        created_at=user.get("created_at", 0.0)
    )

# ==========================================
# DataNode Registry & Heartbeat (ControlNode <-> DataNode)
# ==========================================

@app.post("/nodes/register", response_model=StatusResponse, dependencies=[Depends(verify_internal_auth)])
async def register_datanode(req: NodeRegistrationRequest):
    import time
    node = NodeInfo(
        node_id=req.node_id,
        host=req.host,
        port=req.port,
        url=req.url,
        total_space=req.total_space,
        available_space=req.available_space,
        last_heartbeat=time.time(),
        is_alive=True
    )
    store.register_node(node)
    await sync_to_standby_if_needed()
    return StatusResponse(status="registered", message=f"Node {req.node_id} registered successfully")

@app.post("/nodes/heartbeat", response_model=HeartbeatResponse, dependencies=[Depends(verify_internal_auth)])
async def heartbeat_datanode(req: HeartbeatRequest):
    store.update_heartbeat(req.node_id, req.available_space, req.active_chunks)
    # Check if any chunk needs healing or replication
    return HeartbeatResponse(status="ok")

@app.get("/nodes", response_model=List[NodeInfo])
async def list_datanodes():
    return store.get_alive_nodes()

# ==========================================
# File System Hierarchy & Navigation (RF1)
# ==========================================

@app.get("/fs/ls", response_model=DirectoryListing)
async def list_directory(path: str = "/", username: str = Depends(get_current_user)):
    if not store.check_permission(path, username, "r"):
        raise HTTPException(status_code=403, detail="Permission denied for path")

    success, items, msg = store.list_dir(path)
    if not success:
        raise HTTPException(status_code=404, detail=msg)
    return DirectoryListing(path=store.normalize_path(path), items=items)

@app.post("/fs/mkdir", response_model=StatusResponse)
async def make_directory(req: PathRequest, username: str = Depends(get_current_user)):
    if not ha_mgr.is_primary:
        raise HTTPException(status_code=503, detail="Standby node is read-only")

    success, msg = store.mkdir(req.path, owner=username)
    if not success:
        raise HTTPException(status_code=400, detail=msg)

    await sync_to_standby_if_needed()
    return StatusResponse(status="success", message=msg)

@app.post("/fs/rmdir", response_model=StatusResponse)
async def remove_directory(req: PathRequest, username: str = Depends(get_current_user)):
    if not ha_mgr.is_primary:
        raise HTTPException(status_code=503, detail="Standby node is read-only")
    if not store.check_permission(req.path, username, "w"):
        raise HTTPException(status_code=403, detail="Permission denied")

    success, msg = store.rmdir(req.path)
    if not success:
        raise HTTPException(status_code=400, detail=msg)

    await sync_to_standby_if_needed()
    return StatusResponse(status="success", message=msg)

@app.delete("/fs/rm", response_model=StatusResponse)
async def remove_file_or_dir(req: RmRequest, username: str = Depends(get_current_user)):
    if not ha_mgr.is_primary:
        raise HTTPException(status_code=503, detail="Standby node is read-only")
    if not store.check_permission(req.path, username, "w"):
        raise HTTPException(status_code=403, detail="Permission denied")

    success, chunks_to_clean, msg = store.rm_file(req.path, recursive=req.recursive)
    if not success:
        raise HTTPException(status_code=400, detail=msg)

    # Issue asynchronous deletion of chunks to DataNodes
    if chunks_to_clean:
        async def clean_chunks(chunks: List[str]):
            for node in store.get_alive_nodes():
                for cid in chunks:
                    try:
                        async with httpx.AsyncClient(timeout=3.0) as client:
                            await client.delete(f"{node.url}/chunks/{cid}")
                    except Exception:
                        pass
        asyncio.create_task(clean_chunks(chunks_to_clean))

    await sync_to_standby_if_needed()
    return StatusResponse(status="success", message=msg, detail={"deleted_chunks": len(chunks_to_clean)})

@app.get("/fs/stat", response_model=FileInfo)
async def get_stat(path: str, username: str = Depends(get_current_user)):
    if not store.check_permission(path, username, "r"):
        raise HTTPException(status_code=403, detail="Permission denied")
    info = store.get_file_info(path)
    if not info:
        raise HTTPException(status_code=404, detail=f"Path '{path}' not found")
    return info

# ==========================================
# File Transfer Coordination (RF2, RNF4, RNF5)
# ==========================================

@app.post("/transfer/init-upload", response_model=InitUploadResponse)
async def init_upload(req: InitUploadRequest, username: str = Depends(get_current_user)):
    """
    Allocates chunk layout across alive DataNodes using load balancing (RNF1, RNF4).
    """
    if not ha_mgr.is_primary:
        raise HTTPException(status_code=503, detail="Standby node is read-only")

    norm = store.normalize_path(req.path)
    parent = store.get_parent_path(norm)
    if not store.check_permission(parent, username, "w"):
        raise HTTPException(status_code=403, detail="Permission denied to write to destination directory")

    # Check lock status
    existing = store.get_file_info(norm)
    if existing and existing.is_locked and existing.locked_by != username:
        raise HTTPException(status_code=423, detail=f"File is locked by user '{existing.locked_by}'")

    try:
        upload_id, plans = store.allocate_chunks(
            path=norm,
            file_size=req.file_size,
            chunk_size=req.chunk_size,
            replication_factor=req.replication_factor
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return InitUploadResponse(
        path=norm,
        upload_id=upload_id,
        chunk_size=req.chunk_size,
        total_chunks=len(plans),
        chunks=plans
    )

@app.post("/transfer/commit-upload", response_model=FileInfo)
async def commit_upload(req: CommitUploadRequest, username: str = Depends(get_current_user)):
    """
    Finalizes file upload after client has written chunks to DataNodes.
    """
    if not ha_mgr.is_primary:
        raise HTTPException(status_code=503, detail="Standby node is read-only")

    file_info = store.commit_file(
        path=req.path,
        upload_id=req.upload_id,
        total_size=req.total_size,
        chunks_info=[ch.model_dump() for ch in req.chunks],
        owner=username
    )

    await sync_to_standby_if_needed()
    return file_info

@app.get("/transfer/file-info", response_model=DownloadFileInfoResponse)
async def get_download_file_info(path: str, username: str = Depends(get_current_user)):
    """
    Returns chunk locations and replica nodes for fault-tolerant parallel download (RF2, RNF2, RNF5).
    """
    if not store.check_permission(path, username, "r"):
        raise HTTPException(status_code=403, detail="Permission denied")

    norm = store.normalize_path(path)
    info = store.get_file_info(norm)
    if not info:
        raise HTTPException(status_code=404, detail=f"File '{norm}' not found")
    if info.is_directory:
        raise HTTPException(status_code=400, detail=f"'{norm}' is a directory, not a file")

    alive_nodes = {n.node_id: n for n in store.get_alive_nodes()}
    download_chunks = []

    for ch in info.chunks:
        # Resolve alive DataNodes holding this chunk
        chunk_alive_locations = []
        for loc_id in ch.locations:
            if loc_id in alive_nodes:
                chunk_alive_locations.append(alive_nodes[loc_id])
            elif loc_id in store.nodes:
                # Include even if recently marked dead, as fallback candidate
                chunk_alive_locations.append(store.nodes[loc_id])

        if not chunk_alive_locations:
            raise HTTPException(
                status_code=503,
                detail=f"High Availability failure: No alive DataNodes currently hold chunk {ch.chunk_id}"
            )

        download_chunks.append(ChunkDownloadLocation(
            chunk_id=ch.chunk_id,
            chunk_index=ch.chunk_index,
            size_bytes=ch.size_bytes,
            sha256=ch.sha256,
            locations=chunk_alive_locations
        ))

    return DownloadFileInfoResponse(
        path=norm,
        size_bytes=info.size_bytes,
        total_chunks=len(download_chunks),
        chunks=download_chunks
    )

# ==========================================
# File Locking & Access Primitives (RF3)
# ==========================================

@app.post("/fs/lock", response_model=StatusResponse)
async def lock_file(req: LockAcquireRequest, username: str = Depends(get_current_user)):
    """Acquires lock on file (RF3)."""
    if not ha_mgr.is_primary:
        raise HTTPException(status_code=503, detail="Standby node is read-only")
    if not store.check_permission(req.path, username, "w"):
        raise HTTPException(status_code=403, detail="Permission denied")

    success, msg = store.acquire_lock(req.path, username, req.lock_type)
    if not success:
        raise HTTPException(status_code=423, detail=msg)

    await sync_to_standby_if_needed()
    return StatusResponse(status="locked", message=msg)

@app.post("/fs/unlock", response_model=StatusResponse)
async def unlock_file(req: LockReleaseRequest, username: str = Depends(get_current_user)):
    """Releases lock on file (RF3)."""
    if not ha_mgr.is_primary:
        raise HTTPException(status_code=503, detail="Standby node is read-only")

    success, msg = store.release_lock(req.path, username)
    if not success:
        raise HTTPException(status_code=403, detail=msg)

    await sync_to_standby_if_needed()
    return StatusResponse(status="unlocked", message=msg)

@app.get("/fs/lock/status", response_model=LockStatusResponse)
async def lock_status(path: str, username: str = Depends(get_current_user)):
    info = store.get_file_info(path)
    if not info:
        raise HTTPException(status_code=404, detail="File not found")
    return LockStatusResponse(
        path=store.normalize_path(path),
        is_locked=info.is_locked,
        locked_by=info.locked_by,
        lock_type=info.lock_type
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
