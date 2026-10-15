import os
import io
import time
from typing import List, Optional, Tuple, Dict, Any
import httpx

from common.config import DEFAULT_CHUNK_SIZE, DEFAULT_REPLICATION_FACTOR
from common.crypto import calculate_sha256
from common.models import (
    FileInfo,
    DirectoryListing,
    InitUploadRequest,
    InitUploadResponse,
    CommitUploadRequest,
    CommitChunkInfo,
    DownloadFileInfoResponse,
    NodeInfo
)
from common.logger import setup_logger

logger = setup_logger("DFShaClient")

class DFShaClient:
    """
    Client SDK for DFSha implementing:
    - RF1: File system management (ls, cd, pwd, mkdir, rmdir, rm, stat)
    - RF2: File transfer (put / send, get / receive)
    - RF3: File access primitives (open, read, write, close, lock, unlock, cat)
    - RNF2 & RNF7: Dynamic node discovery, High Availability replica failover
    """
    def __init__(
        self,
        master_url: str = "http://localhost:8000",
        standby_url: Optional[str] = None
    ):
        self.primary_url = master_url.rstrip("/")
        self.standby_url = standby_url.rstrip("/") if standby_url else None
        self.active_master_url = self.primary_url
        self.token: Optional[str] = None
        self.username: Optional[str] = None
        self.cwd: str = "/"

    def _resolve_path(self, path: Optional[str]) -> str:
        """Resolves relative or absolute path against current working directory (RNF7 Transparencia)."""
        if not path or path == ".":
            return self.cwd
        path = path.strip().replace("\\", "/")
        if not path.startswith("/"):
            base = self.cwd if self.cwd.endswith("/") else self.cwd + "/"
            path = base + path

        parts = [p for p in path.split("/") if p and p != "."]
        stack = []
        for p in parts:
            if p == "..":
                if stack:
                    stack.pop()
            else:
                stack.append(p)
        return "/" + "/".join(stack)

    def _get_headers(self) -> Dict[str, str]:
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """
        Executes HTTP request against active ControlNode with automatic HA failover (RNF2).
        """
        url = f"{self.active_master_url}{path}"
        headers = kwargs.pop("headers", {})
        headers.update(self._get_headers())
        timeout = kwargs.pop("timeout", 10.0)

        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.request(method, url, headers=headers, **kwargs)
                return resp
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as e:
            # ControlNode failover logic
            if self.standby_url and self.active_master_url != self.standby_url:
                logger.warning(f"Active ControlNode {self.active_master_url} unreachable. Failing over to standby {self.standby_url}...")
                self.active_master_url = self.standby_url
                url = f"{self.active_master_url}{path}"
                with httpx.Client(timeout=timeout) as client:
                    return client.request(method, url, headers=headers, **kwargs)
            raise e

    # ================= Authentication (RNF6) =================
    def register(self, username: str, password: str, role: str = "user") -> bool:
        resp = self._request("POST", "/auth/register", json={"username": username, "password": password, "role": role})
        if resp.status_code == 200:
            logger.info(f"User '{username}' registered successfully.")
            return True
        raise RuntimeError(f"Registration failed ({resp.status_code}): {resp.text}")

    def login(self, username: str, password: str) -> bool:
        resp = self._request("POST", "/auth/login", json={"username": username, "password": password})
        if resp.status_code == 200:
            data = resp.json()
            self.token = data["access_token"]
            self.username = username
            logger.info(f"User '{username}' logged in successfully.")
            return True
        raise RuntimeError(f"Login failed ({resp.status_code}): {resp.text}")

    def whoami(self) -> dict:
        resp = self._request("GET", "/auth/me")
        if resp.status_code == 200:
            return resp.json()
        raise RuntimeError(f"Failed to fetch profile: {resp.text}")

    # ================= RF1: File System Management =================
    def pwd(self) -> str:
        return self.cwd

    def cd(self, path: str) -> str:
        target = self._resolve_path(path)
        if target == "/":
            self.cwd = "/"
            return self.cwd

        # Verify directory exists
        resp = self._request("GET", f"/fs/stat?path={target}")
        if resp.status_code == 200:
            info = resp.json()
            if not info.get("is_directory"):
                raise ValueError(f"'{target}' is not a directory")
            self.cwd = target
            return self.cwd
        raise FileNotFoundError(f"Directory '{target}' not found")

    def ls(self, path: Optional[str] = None) -> List[FileInfo]:
        target = self._resolve_path(path)
        resp = self._request("GET", f"/fs/ls?path={target}")
        if resp.status_code == 200:
            data = resp.json()
            return [FileInfo(**item) for item in data.get("items", [])]
        raise RuntimeError(f"ls failed ({resp.status_code}): {resp.text}")

    def mkdir(self, path: str) -> bool:
        target = self._resolve_path(path)
        resp = self._request("POST", "/fs/mkdir", json={"path": target})
        if resp.status_code == 200:
            return True
        raise RuntimeError(f"mkdir failed: {resp.text}")

    def rmdir(self, path: str) -> bool:
        target = self._resolve_path(path)
        resp = self._request("POST", "/fs/rmdir", json={"path": target})
        if resp.status_code == 200:
            return True
        raise RuntimeError(f"rmdir failed: {resp.text}")

    def rm(self, path: str, recursive: bool = False) -> bool:
        target = self._resolve_path(path)
        resp = self._request("DELETE", "/fs/rm", json={"path": target, "recursive": recursive})
        if resp.status_code == 200:
            return True
        raise RuntimeError(f"rm failed: {resp.text}")

    def stat(self, path: str) -> FileInfo:
        target = self._resolve_path(path)
        resp = self._request("GET", f"/fs/stat?path={target}")
        if resp.status_code == 200:
            return FileInfo(**resp.json())
        raise FileNotFoundError(f"File or directory '{target}' not found")

    # ================= RF2: File Transfer (put / send, get / receive) =================
    def put(
        self,
        local_file_path: str,
        remote_path: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR
    ) -> FileInfo:
        """
        Sends local file to DFSha cluster (RF2 & RNF4):
        1. Slices file into chunks.
        2. Negotiates chunk layout with ControlNode.
        3. Uploads chunks to DataNodes using pipeline replication.
        4. Commits file metadata.
        """
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file '{local_file_path}' does not exist")

        file_size = os.path.getsize(local_file_path)
        filename = os.path.basename(local_file_path)

        if not remote_path:
            dest_path = self._resolve_path(filename)
        else:
            resolved = self._resolve_path(remote_path)
            # Check if remote path is an existing directory
            try:
                st = self.stat(resolved)
                if st.is_directory:
                    dest_path = f"{resolved.rstrip('/')}/{filename}"
                else:
                    dest_path = resolved
            except Exception:
                dest_path = resolved

        # Step 1: Initialize upload
        init_req = InitUploadRequest(
            path=dest_path,
            file_size=file_size,
            chunk_size=chunk_size,
            replication_factor=replication_factor
        )
        resp = self._request("POST", "/transfer/init-upload", json=init_req.model_dump())
        if resp.status_code != 200:
            raise RuntimeError(f"Init upload failed ({resp.status_code}): {resp.text}")

        plan = InitUploadResponse(**resp.json())
        committed_chunks: List[CommitChunkInfo] = []

        # Step 2: Slice and upload each chunk
        with open(local_file_path, "rb") as f:
            for chunk_plan in plan.chunks:
                chunk_data = f.read(plan.chunk_size)
                chunk_sha256 = calculate_sha256(chunk_data)

                # Prepare pipeline replication URLs
                replica_urls = [r.url for r in chunk_plan.replica_nodes]
                replicate_param = ",".join(replica_urls) if replica_urls else None

                primary_url = chunk_plan.primary_node.url
                uploaded_successfully = False
                candidate_urls = [primary_url] + replica_urls

                # Attempt primary; if fails, fallback directly to replica candidate (RNF2)
                for target_url in candidate_urls:
                    try:
                        params = {"sha256": chunk_sha256}
                        if replicate_param and target_url == primary_url:
                            params["replicate_to"] = replicate_param

                        with httpx.Client(timeout=30.0) as http_client:
                            put_resp = http_client.put(
                                f"{target_url}/chunks/{chunk_plan.chunk_id}",
                                content=chunk_data,
                                params=params
                            )
                            if put_resp.status_code == 200:
                                uploaded_successfully = True
                                break
                    except Exception as ex:
                        logger.warning(f"Upload to {target_url} failed: {ex}. Trying next replica node...")

                if not uploaded_successfully:
                    raise RuntimeError(f"Failed to upload chunk {chunk_plan.chunk_id} to any available DataNode")

                stored_nodes = [chunk_plan.primary_node.node_id] + [rn.node_id for rn in chunk_plan.replica_nodes]
                committed_chunks.append(CommitChunkInfo(
                    chunk_id=chunk_plan.chunk_id,
                    chunk_index=chunk_plan.chunk_index,
                    size_bytes=len(chunk_data),
                    sha256=chunk_sha256,
                    stored_nodes=stored_nodes
                ))

        # Step 3: Commit upload to ControlNode
        commit_req = CommitUploadRequest(
            path=dest_path,
            upload_id=plan.upload_id,
            total_size=file_size,
            chunks=committed_chunks
        )
        commit_resp = self._request("POST", "/transfer/commit-upload", json=commit_req.model_dump())
        if commit_resp.status_code != 200:
            raise RuntimeError(f"Commit upload failed ({commit_resp.status_code}): {commit_resp.text}")

        logger.info(f"Successfully uploaded '{dest_path}' ({file_size} bytes in {len(committed_chunks)} chunks)")
        return FileInfo(**commit_resp.json())

    def get(self, remote_path: str, local_path: Optional[str] = None) -> bytes:
        """
        Retrieves file from DFSha cluster (RF2):
        1. Inquires chunk locations from ControlNode.
        2. Downloads chunks in parallel / sequentially from alive replicas (with HA failover!).
        3. Verifies SHA-256 integrity (RNF3).
        4. Reassembles file.
        """
        dest_path = self._resolve_path(remote_path)
        resp = self._request("GET", f"/transfer/file-info?path={dest_path}")
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to get file info ({resp.status_code}): {resp.text}")

        file_meta = DownloadFileInfoResponse(**resp.json())
        chunks_content: List[bytes] = [b""] * file_meta.total_chunks

        for ch in file_meta.chunks:
            chunk_downloaded = False
            # Iterate through available replica nodes (RNF2 - Alta Disponibilidad)
            for node in ch.locations:
                try:
                    with httpx.Client(timeout=20.0) as http_client:
                        get_resp = http_client.get(f"{node.url}/chunks/{ch.chunk_id}")
                        if get_resp.status_code == 200:
                            data = get_resp.content
                            actual_sha256 = calculate_sha256(data)
                            if actual_sha256.lower() != ch.sha256.lower():
                                logger.error(f"SHA-256 mismatch for chunk {ch.chunk_id} from {node.url}!")
                                continue
                            chunks_content[ch.chunk_index] = data
                            chunk_downloaded = True
                            break
                except Exception as ex:
                    logger.warning(f"Error fetching chunk {ch.chunk_id} from {node.url}: {ex}. Trying next replica...")

            if not chunk_downloaded:
                raise RuntimeError(f"High Availability Failure: Could not retrieve chunk {ch.chunk_id} from any replica")

        full_content = b"".join(chunks_content)

        if local_path:
            # If local_path is a directory, save with remote file basename
            if os.path.isdir(local_path):
                filename = os.path.basename(dest_path)
                target_file = os.path.join(local_path, filename)
            else:
                target_file = local_path
            with open(target_file, "wb") as f:
                f.write(full_content)
            logger.info(f"File '{dest_path}' saved locally to '{target_file}' ({len(full_content)} bytes)")

        return full_content

    # Aliases matching prompt requirements RF2
    send = put
    receive = get

    # ================= RF3: File Access Primitives & Locking =================
    def cat(self, remote_path: str) -> str:
        """Reads file content and returns it as a decoded string (RF3)."""
        content = self.get(remote_path)
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            return f"<Binary data: {len(content)} bytes>"

    def lock(self, remote_path: str, lock_type: str = "exclusive") -> bool:
        """Acquires lock on file (RF3)."""
        target = self._resolve_path(remote_path)
        resp = self._request("POST", "/fs/lock", json={"path": target, "lock_type": lock_type})
        if resp.status_code == 200:
            return True
        raise RuntimeError(f"Lock failed: {resp.text}")

    def unlock(self, remote_path: str) -> bool:
        """Releases lock on file (RF3)."""
        target = self._resolve_path(remote_path)
        resp = self._request("POST", "/fs/unlock", json={"path": target})
        if resp.status_code == 200:
            return True
        raise RuntimeError(f"Unlock failed: {resp.text}")

    def lock_status(self, remote_path: str) -> dict:
        target = self._resolve_path(remote_path)
        resp = self._request("GET", f"/fs/lock/status?path={target}")
        if resp.status_code == 200:
            return resp.json()
        raise RuntimeError(f"Lock status query failed: {resp.text}")

    def open(self, remote_path: str, mode: str = "r") -> "DFSFile":
        """Returns a DFSFile file-like object supporting read(), write(), close() (RF3)."""
        return DFSFile(self, remote_path, mode)

    def list_nodes(self) -> List[NodeInfo]:
        """Lists active DataNodes in the cluster."""
        resp = self._request("GET", "/nodes")
        if resp.status_code == 200:
            return [NodeInfo(**n) for n in resp.json()]
        raise RuntimeError(f"Failed to query nodes: {resp.text}")

    def cluster_health(self) -> dict:
        """Queries cluster health status."""
        resp = self._request("GET", "/health")
        if resp.status_code == 200:
            return resp.json()
        raise RuntimeError(f"Health query failed: {resp.text}")


class DFSFile:
    """
    File-like object abstraction for DFSha files (RF3: open, close, read, write, lock).
    """
    def __init__(self, client: DFShaClient, path: str, mode: str = "r"):
        self.client = client
        self.path = path
        self.mode = mode
        self.closed = False
        self.buffer = io.BytesIO()

        if "r" in mode or "a" in mode:
            try:
                content = self.client.get(path)
                self.buffer.write(content)
                if "r" in mode:
                    self.buffer.seek(0)
            except Exception:
                if "r" in mode:
                    raise FileNotFoundError(f"File '{path}' not found for reading")

        if "w" in mode:
            # Overwrite mode
            self.buffer = io.BytesIO()

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self.buffer.read(size)

    def write(self, data: bytes) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if "w" not in self.mode and "a" not in self.mode:
            raise IOError("File not open for writing")
        return self.buffer.write(data)

    def flush(self):
        if self.closed:
            return
        if "w" in self.mode or "a" in self.mode:
            # Write buffer content to temporary file and upload
            import tempfile
            with tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
                tmp.write(self.buffer.getvalue())
                tmp_path = tmp.name

            try:
                self.client.put(tmp_path, self.path)
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    def close(self):
        if not self.closed:
            self.flush()
            self.closed = True
            self.buffer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
