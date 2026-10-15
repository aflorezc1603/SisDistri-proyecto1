import asyncio
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional, List
import httpx
from fastapi import FastAPI, HTTPException, Request, Response, Header, Query
from fastapi.responses import JSONResponse

from common.config import (
    DATA_DIR,
    HEARTBEAT_INTERVAL_SEC,
    CLUSTER_SECRET,
    CLUSTER_AUTH_HEADER
)
from common.logger import setup_logger
from common.models import NodeRegistrationRequest, HeartbeatRequest
from datanode.storage import ChunkStorageManager

NODE_ID = os.getenv("DFSHA_NODE_ID", f"datanode-{os.getpid()}")
HOST = os.getenv("DFSHA_HOST", "0.0.0.0")
PORT = int(os.getenv("DFSHA_PORT", "8001"))
PUBLIC_URL = os.getenv("DFSHA_PUBLIC_URL", f"http://localhost:{PORT}")
CONTROLNODE_URL = os.getenv("DFSHA_CONTROLNODE_URL", "http://localhost:8000")
CONTROLNODE_STANDBY_URL = os.getenv("DFSHA_CONTROLNODE_STANDBY_URL", "")

logger = setup_logger(f"DataNode-{NODE_ID}")
storage = ChunkStorageManager(os.getenv("DFSHA_DATA_DIR", os.path.join(DATA_DIR, NODE_ID)))

async def register_with_master(master_url: str) -> bool:
    """Registers this DataNode with a ControlNode."""
    total, free = storage.get_storage_stats()
    reg = NodeRegistrationRequest(
        node_id=NODE_ID,
        host=HOST,
        port=PORT,
        url=PUBLIC_URL,
        total_space=total,
        available_space=free
    )
    headers = {CLUSTER_AUTH_HEADER: CLUSTER_SECRET}
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            resp = await client.post(f"{master_url}/nodes/register", json=reg.model_dump(), headers=headers)
            if resp.status_code == 200:
                logger.info(f"Successfully registered with ControlNode at {master_url}")
                return True
            else:
                logger.warning(f"Registration failed at {master_url}: {resp.status_code} - {resp.text}")
    except Exception as e:
        logger.warning(f"Could not reach ControlNode at {master_url}: {e}")
    return False

async def send_heartbeat(master_url: str) -> Optional[dict]:
    """Sends periodic heartbeat to ControlNode with chunk inventory and capacity."""
    _, free = storage.get_storage_stats()
    chunks = storage.list_chunks()
    hb = HeartbeatRequest(
        node_id=NODE_ID,
        available_space=free,
        active_chunks=chunks
    )
    headers = {CLUSTER_AUTH_HEADER: CLUSTER_SECRET}
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            resp = await client.post(f"{master_url}/nodes/heartbeat", json=hb.model_dump(), headers=headers)
            if resp.status_code == 200:
                return resp.json()
    except Exception:
        pass
    return None

async def heartbeat_loop():
    """Background heartbeat task with automatic failover to Standby ControlNode (RNF2)."""
    current_master = CONTROLNODE_URL
    while True:
        try:
            res = await send_heartbeat(current_master)
            if res is None and CONTROLNODE_STANDBY_URL:
                # Primary failed, attempt standby ControlNode
                res = await send_heartbeat(CONTROLNODE_STANDBY_URL)
                if res is not None:
                    current_master = CONTROLNODE_STANDBY_URL
                    logger.info(f"Switched active heartbeat target to standby ControlNode: {CONTROLNODE_STANDBY_URL}")

            if res and "replicate_commands" in res:
                # Handle S2S replication directives
                for cmd in res.get("replicate_commands", []):
                    asyncio.create_task(handle_replicate_command(cmd))
        except Exception as e:
            logger.error(f"Error in heartbeat loop: {e}")

        await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)

async def handle_replicate_command(cmd: dict):
    """Handles master directive to replicate chunk to another DataNode."""
    chunk_id = cmd.get("chunk_id")
    target_url = cmd.get("target_url")
    if not chunk_id or not target_url:
        return
    try:
        data, sha256 = storage.read_chunk(chunk_id)
        headers = {CLUSTER_AUTH_HEADER: CLUSTER_SECRET}
        params = {"sha256": sha256}
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.put(f"{target_url}/chunks/{chunk_id}", content=data, headers=headers, params=params)
            if resp.status_code == 200:
                logger.info(f"Successfully replicated chunk {chunk_id} to {target_url}")
    except Exception as e:
        logger.error(f"Failed to execute replication command for {chunk_id} -> {target_url}: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Register with Primary and Standby if configured
    await register_with_master(CONTROLNODE_URL)
    if CONTROLNODE_STANDBY_URL:
        await register_with_master(CONTROLNODE_STANDBY_URL)

    # Start background heartbeat task
    hb_task = asyncio.create_task(heartbeat_loop())
    yield
    hb_task.cancel()

app = FastAPI(title=f"DFSha DataNode - {NODE_ID}", lifespan=lifespan)

@app.get("/health")
async def health_check():
    _, free = storage.get_storage_stats()
    return {
        "status": "healthy",
        "node_id": NODE_ID,
        "available_space": free,
        "chunks_count": len(storage.list_chunks())
    }

@app.get("/chunks")
async def list_node_chunks():
    """Lists all chunk IDs stored on this DataNode."""
    return {
        "node_id": NODE_ID,
        "chunks": storage.list_chunks()
    }

@app.put("/chunks/{chunk_id}")
async def put_chunk(
    chunk_id: str,
    request: Request,
    sha256: Optional[str] = Query(None),
    replicate_to: Optional[str] = Query(None)
):
    """
    Receives a raw chunk from Client or peer DataNode, encrypts it (AES-256-GCM),
    verifies SHA-256, and pipelines replication to downstream DataNodes (RF2, RNF2, RNF5).
    """
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty chunk payload received")

    try:
        size, actual_sha256 = storage.save_chunk(chunk_id, body, expected_sha256=sha256)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to store chunk: {e}")

    # Pipeline replication (S2S: DataNode -> DataNode)
    # replicate_to is a comma-separated list of peer DataNode URLs: "http://datanode2:8002,http://datanode3:8003"
    if replicate_to:
        next_nodes = [n.strip() for n in replicate_to.split(",") if n.strip()]
        if next_nodes:
            next_target = next_nodes[0]
            remaining_nodes = ",".join(next_nodes[1:])
            try:
                headers = {CLUSTER_AUTH_HEADER: CLUSTER_SECRET}
                params = {"sha256": actual_sha256}
                if remaining_nodes:
                    params["replicate_to"] = remaining_nodes

                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.put(
                        f"{next_target}/chunks/{chunk_id}",
                        content=body,
                        headers=headers,
                        params=params
                    )
                    if resp.status_code != 200:
                        logger.warning(f"Pipeline replication to {next_target} failed: {resp.status_code}")
                    else:
                        logger.info(f"Pipeline replication forward to {next_target} succeeded for chunk {chunk_id}")
            except Exception as e:
                logger.error(f"Error forwarding chunk {chunk_id} to {next_target}: {e}")

    return {
        "status": "success",
        "node_id": NODE_ID,
        "chunk_id": chunk_id,
        "size_bytes": size,
        "sha256": actual_sha256
    }

@app.get("/chunks/{chunk_id}")
async def get_chunk(chunk_id: str):
    """
    Reads, decrypts (AES-256-GCM) and verifies SHA-256 of requested chunk (RF2, RF3).
    """
    try:
        plaintext_data, sha256_hash = storage.read_chunk(chunk_id)
        return Response(
            content=plaintext_data,
            media_type="application/octet-stream",
            headers={"X-Chunk-SHA256": sha256_hash}
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Chunk {chunk_id} not found on this node")
    except Exception as e:
        logger.error(f"Error reading chunk {chunk_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read chunk: {e}")

@app.delete("/chunks/{chunk_id}")
async def delete_chunk(chunk_id: str):
    success = storage.delete_chunk(chunk_id)
    if not success:
        raise HTTPException(status_code=404, detail="Chunk not found")
    return {"status": "deleted", "chunk_id": chunk_id}

@app.post("/internal/replicate")
async def manual_replicate(payload: dict):
    """Direct S2S trigger to replicate a chunk to another node."""
    chunk_id = payload.get("chunk_id")
    target_url = payload.get("target_url")
    if not chunk_id or not target_url:
        raise HTTPException(status_code=400, detail="Missing chunk_id or target_url")
    await handle_replicate_command({"chunk_id": chunk_id, "target_url": target_url})
    return {"status": "initiated", "chunk_id": chunk_id, "target_url": target_url}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
