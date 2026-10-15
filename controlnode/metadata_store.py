import json
import os
import time
from typing import Dict, List, Optional, Tuple, Any
from common.config import METADATA_DIR, DEFAULT_CHUNK_SIZE, DEFAULT_REPLICATION_FACTOR
from common.logger import setup_logger
from common.models import FileInfo, ChunkMetadata, NodeInfo
from common.security import hash_password

logger = setup_logger("MetadataStore")

class MetadataStore:
    """
    In-memory hierarchical filesystem metadata store with JSON persistence,
    user management, access control lists (ACL), and block mapping.
    """
    def __init__(self, storage_dir: str = METADATA_DIR):
        self.storage_dir = os.path.abspath(storage_dir)
        os.makedirs(self.storage_dir, exist_ok=True)
        self.meta_file = os.path.join(self.storage_dir, "fs_metadata.json")

        self.users: Dict[str, Dict[str, Any]] = {}  # username -> {password_hash, role, created_at}
        self.nodes: Dict[str, NodeInfo] = {}  # node_id -> NodeInfo
        # fs_tree maps normalized path -> FileInfo dict
        self.fs_tree: Dict[str, dict] = {}
        self.locks: Dict[str, dict] = {}  # path -> {locked_by, lock_type, acquired_at}
        self.uploads: Dict[str, dict] = {}  # upload_id -> upload plan info

        self.load_from_disk()
        self._ensure_defaults()

    def _ensure_defaults(self):
        # Create default admin user if no users exist
        if "admin" not in self.users:
            self.users["admin"] = {
                "username": "admin",
                "password_hash": hash_password("admin123"),
                "role": "admin",
                "created_at": time.time()
            }
        # Ensure root directory "/" exists
        if "/" not in self.fs_tree:
            now = time.time()
            self.fs_tree["/"] = {
                "path": "/",
                "name": "/",
                "is_directory": True,
                "size_bytes": 0,
                "owner": "admin",
                "permissions": "rwxr-xr-x",
                "created_at": now,
                "modified_at": now,
                "chunks": [],
                "is_locked": False,
                "locked_by": None,
                "lock_type": None
            }
            self.save_to_disk()

    def normalize_path(self, path: str) -> str:
        """Normalizes file system path e.g. '///a/b/../c/' -> '/a/c'"""
        if not path:
            return "/"
        path = path.strip().replace("\\", "/")
        if not path.startswith("/"):
            path = "/" + path
        parts = [p for p in path.split("/") if p and p != "."]
        stack = []
        for p in parts:
            if p == "..":
                if stack:
                    stack.pop()
            else:
                stack.append(p)
        res = "/" + "/".join(stack)
        return res

    def save_to_disk(self):
        """Persists snapshot to disk for durability."""
        try:
            snapshot = {
                "users": self.users,
                "fs_tree": self.fs_tree,
                "locks": self.locks
            }
            temp_path = self.meta_file + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, indent=2)
            os.replace(temp_path, self.meta_file)
        except Exception as e:
            logger.error(f"Failed to persist metadata: {e}")

    def load_from_disk(self):
        """Loads snapshot from disk if present."""
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file, "r", encoding="utf-8") as f:
                    snapshot = json.load(f)
                    self.users = snapshot.get("users", {})
                    self.fs_tree = snapshot.get("fs_tree", {})
                    self.locks = snapshot.get("locks", {})
                logger.info(f"Loaded metadata snapshot ({len(self.fs_tree)} items, {len(self.users)} users)")
            except Exception as e:
                logger.error(f"Failed to load metadata from disk: {e}")

    # ================= User & ACL Methods =================
    def add_user(self, username: str, password_hash: str, role: str = "user") -> bool:
        if username in self.users:
            return False
        self.users[username] = {
            "username": username,
            "password_hash": password_hash,
            "role": role,
            "created_at": time.time()
        }
        self.save_to_disk()
        return True

    def get_user(self, username: str) -> Optional[dict]:
        return self.users.get(username)

    def check_permission(self, path: str, username: str, req_permission: str = "r") -> bool:
        """
        ACL / Permission check (RNF6).
        Admin has full rights.
        File owner has user permissions.
        Others have world permissions.
        """
        user = self.get_user(username)
        if not user:
            return False
        if user.get("role") == "admin":
            return True

        normalized = self.normalize_path(path)
        item = self.fs_tree.get(normalized)
        if not item:
            # Check parent permission for creation
            parent = self.get_parent_path(normalized)
            item = self.fs_tree.get(parent)
            if not item:
                return False

        owner = item.get("owner", "admin")
        perms = item.get("permissions", "rw-r--r--")  # Format: rwxr-xr-x or rw-r--r--

        if username == owner:
            # Check user permission bits (first 3: rwx)
            user_bits = perms[:3]
            return req_permission in user_bits
        else:
            # Check others permission bits (last 3: rwx)
            other_bits = perms[-3:]
            return req_permission in other_bits

    # ================= Path & FS Hierarchy (RF1) =================
    def get_parent_path(self, path: str) -> str:
        norm = self.normalize_path(path)
        if norm == "/":
            return "/"
        parent = os.path.dirname(norm).replace("\\", "/")
        return parent if parent else "/"

    def exists(self, path: str) -> bool:
        norm = self.normalize_path(path)
        return norm in self.fs_tree

    def get_file_info(self, path: str) -> Optional[FileInfo]:
        norm = self.normalize_path(path)
        item = self.fs_tree.get(norm)
        if not item:
            return None
        return FileInfo(**item)

    def mkdir(self, path: str, owner: str = "admin", permissions: str = "rwxr-xr-x") -> Tuple[bool, str]:
        norm = self.normalize_path(path)
        if norm == "/":
            return False, "Root directory already exists"
        if norm in self.fs_tree:
            return False, f"Directory '{norm}' already exists"

        parent = self.get_parent_path(norm)
        if parent != "/" and parent not in self.fs_tree:
            # Recursively create parent if needed
            self.mkdir(parent, owner=owner, permissions=permissions)

        now = time.time()
        name = os.path.basename(norm)
        self.fs_tree[norm] = {
            "path": norm,
            "name": name,
            "is_directory": True,
            "size_bytes": 0,
            "owner": owner,
            "permissions": permissions,
            "created_at": now,
            "modified_at": now,
            "chunks": [],
            "is_locked": False,
            "locked_by": None,
            "lock_type": None
        }
        self.save_to_disk()
        return True, f"Directory '{norm}' created successfully"

    def list_dir(self, path: str) -> Tuple[bool, List[FileInfo], str]:
        norm = self.normalize_path(path)
        item = self.fs_tree.get(norm)
        if not item:
            return False, [], f"Path '{norm}' does not exist"
        if not item.get("is_directory"):
            return False, [], f"Path '{norm}' is not a directory"

        prefix = norm if norm.endswith("/") else norm + "/"
        results = []
        for p, data in self.fs_tree.items():
            if p == norm:
                continue
            if p.startswith(prefix):
                # Only direct children
                relative = p[len(prefix):]
                if "/" not in relative:
                    results.append(FileInfo(**data))

        # Sort directories first, then alphabetical
        results.sort(key=lambda x: (not x.is_directory, x.name.lower()))
        return True, results, "Success"

    def rmdir(self, path: str) -> Tuple[bool, str]:
        norm = self.normalize_path(path)
        if norm == "/":
            return False, "Cannot delete root directory"
        item = self.fs_tree.get(norm)
        if not item:
            return False, f"Directory '{norm}' does not exist"
        if not item.get("is_directory"):
            return False, f"'{norm}' is a file, use rm instead"

        prefix = norm if norm.endswith("/") else norm + "/"
        for p in self.fs_tree:
            if p != norm and p.startswith(prefix):
                return False, f"Directory '{norm}' is not empty"

        del self.fs_tree[norm]
        self.save_to_disk()
        return True, f"Directory '{norm}' deleted"

    def rm_file(self, path: str, recursive: bool = False) -> Tuple[bool, List[str], str]:
        """
        Removes a file or directory.
        Returns (success, list_of_chunk_ids_to_clean, message).
        """
        norm = self.normalize_path(path)
        if norm == "/":
            return False, [], "Cannot delete root directory"
        item = self.fs_tree.get(norm)
        if not item:
            return False, [], f"Path '{norm}' does not exist"

        if item.get("is_locked"):
            return False, [], f"File '{norm}' is locked by user '{item.get('locked_by')}'"

        chunks_to_delete = []
        if item.get("is_directory"):
            if not recursive:
                # Must be empty
                prefix = norm if norm.endswith("/") else norm + "/"
                for p in self.fs_tree:
                    if p != norm and p.startswith(prefix):
                        return False, [], f"Directory '{norm}' is not empty (use recursive=true)"
                del self.fs_tree[norm]
            else:
                prefix = norm if norm.endswith("/") else norm + "/"
                keys_to_del = [p for p in self.fs_tree if p == norm or p.startswith(prefix)]
                for k in keys_to_del:
                    it = self.fs_tree[k]
                    for ch in it.get("chunks", []):
                        chunks_to_delete.append(ch["chunk_id"])
                    del self.fs_tree[k]
        else:
            for ch in item.get("chunks", []):
                chunks_to_delete.append(ch["chunk_id"])
            del self.fs_tree[norm]

        self.save_to_disk()
        return True, chunks_to_delete, f"Removed '{norm}'"

    # ================= File Locking (RF3) =================
    def acquire_lock(self, path: str, username: str, lock_type: str = "exclusive") -> Tuple[bool, str]:
        norm = self.normalize_path(path)
        item = self.fs_tree.get(norm)
        if not item:
            return False, f"File '{norm}' does not exist"
        if item.get("is_directory"):
            return False, "Cannot lock directories"

        if item.get("is_locked"):
            if item.get("locked_by") == username:
                return True, f"Lock already held by user '{username}'"
            return False, f"File '{norm}' is currently locked by '{item.get('locked_by')}'"

        item["is_locked"] = True
        item["locked_by"] = username
        item["lock_type"] = lock_type
        self.locks[norm] = {
            "locked_by": username,
            "lock_type": lock_type,
            "acquired_at": time.time()
        }
        self.save_to_disk()
        return True, f"Lock acquired on '{norm}' by '{username}'"

    def release_lock(self, path: str, username: str) -> Tuple[bool, str]:
        norm = self.normalize_path(path)
        item = self.fs_tree.get(norm)
        if not item:
            return False, f"File '{norm}' does not exist"

        if not item.get("is_locked"):
            return True, "File was not locked"

        user = self.get_user(username)
        is_admin = user and user.get("role") == "admin"
        if item.get("locked_by") != username and not is_admin:
            return False, f"Cannot release lock: owned by '{item.get('locked_by')}'"

        item["is_locked"] = False
        item["locked_by"] = None
        item["lock_type"] = None
        self.locks.pop(norm, None)
        self.save_to_disk()
        return True, f"Lock released on '{norm}'"

    # ================= Node Registry (DataNodes) =================
    def register_node(self, node: NodeInfo):
        self.nodes[node.node_id] = node
        logger.info(f"Registered DataNode '{node.node_id}' at {node.url}")

    def update_heartbeat(self, node_id: str, available_space: int, active_chunks: List[str]):
        if node_id in self.nodes:
            self.nodes[node_id].available_space = available_space
            self.nodes[node_id].last_heartbeat = time.time()
            self.nodes[node_id].is_alive = True

    def get_alive_nodes(self, timeout_sec: float = 15.0) -> List[NodeInfo]:
        now = time.time()
        alive = []
        for nid, node in list(self.nodes.items()):
            if (now - node.last_heartbeat) <= timeout_sec:
                node.is_alive = True
                alive.append(node)
            else:
                node.is_alive = False
        return alive

    # ================= File Upload & Chunk Placement =================
    def allocate_chunks(
        self,
        path: str,
        file_size: int,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR
    ) -> Tuple[str, List[dict]]:
        """
        Calculates chunk layout and assigns DataNodes using load balancing (RNF1, RNF4, RNF5).
        Returns (upload_id, chunk_plans).
        """
        import uuid
        upload_id = str(uuid.uuid4())
        norm = self.normalize_path(path)

        alive_nodes = self.get_alive_nodes()
        if not alive_nodes:
            raise RuntimeError("No active DataNodes available in the DFSha cluster")

        # Sort alive nodes by available space descending (Load Balancing)
        alive_nodes.sort(key=lambda n: n.available_space, reverse=True)

        total_chunks = (file_size + chunk_size - 1) // chunk_size if file_size > 0 else 1
        chunk_plans = []
        eff_rep = min(len(alive_nodes), replication_factor)

        for i in range(total_chunks):
            chunk_id = f"chunk_{uuid.uuid4().hex[:12]}_{i}"
            expected_size = min(chunk_size, file_size - i * chunk_size) if file_size > 0 else 0

            # Distribute replicas across distinct nodes using round-robin / capacity offset
            selected_nodes = []
            for r in range(eff_rep):
                idx = (i + r) % len(alive_nodes)
                selected_nodes.append(alive_nodes[idx])

            primary = selected_nodes[0]
            replicas = selected_nodes[1:]

            chunk_plans.append({
                "chunk_id": chunk_id,
                "chunk_index": i,
                "expected_size": expected_size,
                "primary_node": primary.model_dump(),
                "replica_nodes": [rn.model_dump() for rn in replicas]
            })

        self.uploads[upload_id] = {
            "path": norm,
            "file_size": file_size,
            "chunk_size": chunk_size,
            "total_chunks": total_chunks,
            "created_at": time.time(),
            "plans": chunk_plans
        }
        return upload_id, chunk_plans

    def commit_file(
        self,
        path: str,
        upload_id: str,
        total_size: int,
        chunks_info: List[dict],
        owner: str = "admin",
        permissions: str = "rw-r--r--"
    ) -> FileInfo:
        norm = self.normalize_path(path)
        parent = self.get_parent_path(norm)
        if parent != "/" and parent not in self.fs_tree:
            self.mkdir(parent, owner=owner)

        now = time.time()
        name = os.path.basename(norm)

        # Build chunks list
        stored_chunks = []
        for ch in chunks_info:
            stored_chunks.append({
                "chunk_id": ch["chunk_id"],
                "chunk_index": ch["chunk_index"],
                "size_bytes": ch["size_bytes"],
                "sha256": ch["sha256"],
                "locations": ch["stored_nodes"]
            })

        self.fs_tree[norm] = {
            "path": norm,
            "name": name,
            "is_directory": False,
            "size_bytes": total_size,
            "owner": owner,
            "permissions": permissions,
            "created_at": now,
            "modified_at": now,
            "chunks": stored_chunks,
            "is_locked": False,
            "locked_by": None,
            "lock_type": None
        }
        self.uploads.pop(upload_id, None)
        self.save_to_disk()
        return FileInfo(**self.fs_tree[norm])

    # ================= State Sync for HA (RNF2) =================
    def get_state_snapshot(self) -> dict:
        return {
            "users": self.users,
            "fs_tree": self.fs_tree,
            "locks": self.locks,
            "nodes": {nid: n.model_dump() for nid, n in self.nodes.items()}
        }

    def apply_state_snapshot(self, snapshot: dict):
        self.users = snapshot.get("users", {})
        self.fs_tree = snapshot.get("fs_tree", {})
        self.locks = snapshot.get("locks", {})
        raw_nodes = snapshot.get("nodes", {})
        self.nodes = {nid: NodeInfo(**data) for nid, data in raw_nodes.items()}
        self.save_to_disk()
        logger.info("Successfully applied HA state snapshot from Primary ControlNode")
