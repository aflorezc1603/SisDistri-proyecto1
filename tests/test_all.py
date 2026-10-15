import os
import sys
import time
import subprocess
import shutil
import tempfile
import pytest
import httpx

from client.sdk import DFShaClient
from common.crypto import calculate_sha256, decrypt_block
from common.config import DEFAULT_AES_KEY

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TEST_DATA_DIR = os.path.join(BASE_DIR, "test_cluster_data")

MASTER_PORT = 8900
STANDBY_PORT = 8910
DN1_PORT = 8901
DN2_PORT = 8902
DN3_PORT = 8903

processes = []

def wait_for_url(url: str, timeout: float = 12.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        try:
            with httpx.Client(timeout=1.0) as client:
                r = client.get(url)
                if r.status_code == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.3)
    return False

def setup_local_cluster():
    """Starts an isolated in-memory/disk cluster for automated testing."""
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)
    os.makedirs(TEST_DATA_DIR, exist_ok=True)

    py_exe = sys.executable
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR
    env["DFSHA_CHUNK_SIZE"] = str(512 * 1024)  # 512 KB chunks for testing
    env["DFSHA_REPLICATION_FACTOR"] = "2"

    # 1. Primary ControlNode
    p_env = env.copy()
    p_env["DFSHA_PORT"] = str(MASTER_PORT)
    p_env["DFSHA_ROLE"] = "primary"
    p_env["DFSHA_STANDBY_URL"] = f"http://127.0.0.1:{STANDBY_PORT}"
    p_env["DFSHA_METADATA_DIR"] = os.path.join(TEST_DATA_DIR, "meta_primary")
    p1 = subprocess.Popen([py_exe, "-m", "uvicorn", "controlnode.main:app", "--host", "127.0.0.1", "--port", str(MASTER_PORT)], env=p_env)
    processes.append(p1)

    # 2. Standby ControlNode
    s_env = env.copy()
    s_env["DFSHA_PORT"] = str(STANDBY_PORT)
    s_env["DFSHA_ROLE"] = "standby"
    s_env["DFSHA_PRIMARY_URL"] = f"http://127.0.0.1:{MASTER_PORT}"
    s_env["DFSHA_METADATA_DIR"] = os.path.join(TEST_DATA_DIR, "meta_standby")
    s_env["DFSHA_HA_SYNC_INTERVAL"] = "1.0"
    p2 = subprocess.Popen([py_exe, "-m", "uvicorn", "controlnode.main:app", "--host", "127.0.0.1", "--port", str(STANDBY_PORT)], env=s_env)
    processes.append(p2)

    # 3. DataNodes 1, 2, 3
    for i, port in enumerate([DN1_PORT, DN2_PORT, DN3_PORT], start=1):
        d_env = env.copy()
        nid = f"test-dn-{i}"
        d_env["DFSHA_NODE_ID"] = nid
        d_env["DFSHA_PORT"] = str(port)
        d_env["DFSHA_PUBLIC_URL"] = f"http://127.0.0.1:{port}"
        d_env["DFSHA_CONTROLNODE_URL"] = f"http://127.0.0.1:{MASTER_PORT}"
        d_env["DFSHA_CONTROLNODE_STANDBY_URL"] = f"http://127.0.0.1:{STANDBY_PORT}"
        d_env["DFSHA_DATA_DIR"] = os.path.join(TEST_DATA_DIR, nid)
        p = subprocess.Popen([py_exe, "-m", "uvicorn", "datanode.main:app", "--host", "127.0.0.1", "--port", str(port)], env=d_env)
        processes.append(p)

    # Wait for all nodes to be ready
    assert wait_for_url(f"http://127.0.0.1:{MASTER_PORT}/health"), "Primary ControlNode failed to start"
    assert wait_for_url(f"http://127.0.0.1:{STANDBY_PORT}/health"), "Standby ControlNode failed to start"
    assert wait_for_url(f"http://127.0.0.1:{DN1_PORT}/health"), "DataNode 1 failed to start"
    assert wait_for_url(f"http://127.0.0.1:{DN2_PORT}/health"), "DataNode 2 failed to start"
    assert wait_for_url(f"http://127.0.0.1:{DN3_PORT}/health"), "DataNode 3 failed to start"

    # Give nodes 2 seconds to register heartbeats
    time.sleep(2)

def teardown_local_cluster():
    for p in processes:
        try:
            p.terminate()
            p.wait(timeout=2.0)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass
    processes.clear()
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)


class TestDFShaSuite:

    @classmethod
    def setup_class(cls):
        setup_local_cluster()
        cls.client = DFShaClient(
            master_url=f"http://127.0.0.1:{MASTER_PORT}",
            standby_url=f"http://127.0.0.1:{STANDBY_PORT}"
        )
        cls.client.login("admin", "admin123")

    @classmethod
    def teardown_class(cls):
        teardown_local_cluster()

    def test_01_authentication_and_rbac(self):
        """Tests user registration, login, and JWT access tokens (RNF6)."""
        # Register new user
        reg_ok = self.client.register("student1", "telematica2026", role="user")
        assert reg_ok is True

        # Test login
        user_client = DFShaClient(master_url=f"http://127.0.0.1:{MASTER_PORT}")
        assert user_client.login("student1", "telematica2026") is True
        profile = user_client.whoami()
        assert profile["username"] == "student1"
        assert profile["role"] == "user"

        # Invalid login check
        with pytest.raises(Exception):
            user_client.login("student1", "wrong_password")

    def test_02_filesystem_management_rf1(self):
        """Tests RF1: mkdir, ls, cd, pwd, rmdir."""
        # Create directories
        assert self.client.mkdir("/courses") is True
        assert self.client.mkdir("/courses/distributed") is True

        # List /courses
        items = self.client.ls("/courses")
        assert len(items) >= 1
        assert any(it.name == "distributed" and it.is_directory for it in items)

        # cd & pwd
        self.client.cd("/courses/distributed")
        assert self.client.pwd() == "/courses/distributed"

        # Back to root
        self.client.cd("/")
        assert self.client.pwd() == "/"

    def test_03_file_transfer_partitioning_and_encryption_rf2_rnf4_rnf6(self):
        """
        Tests RF2 & RNF4:
        Uploads a 1.2 MB file (which gets sliced into 3 chunks with 512KB chunk size),
        verifies chunk distribution, replication (R=2), AES-256-GCM encryption on disk,
        and retrieves the file verifying exact SHA-256 byte match.
        """
        # Create a test payload of 1.2 MB
        chunk_size = 512 * 1024
        file_size = int(1.2 * 1024 * 1024)
        sample_bytes = os.urandom(file_size)
        original_sha = calculate_sha256(sample_bytes)

        with tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
            tmp.write(sample_bytes)
            tmp_local_path = tmp.name

        try:
            # Upload (put)
            remote_path = "/courses/distributed/project1_data.bin"
            file_info = self.client.put(tmp_local_path, remote_path, chunk_size=chunk_size, replication_factor=2)

            assert file_info.path == remote_path
            assert file_info.size_bytes == file_size
            assert len(file_info.chunks) == 3, f"Expected 3 chunks, got {len(file_info.chunks)}"

            # Verify replication: each chunk must have 2 replica locations
            for ch in file_info.chunks:
                assert len(ch.locations) == 2, f"Expected 2 replica locations for chunk {ch.chunk_id}"

            # Verify RNF6: Encryption at rest on DataNode disk!
            # Find chunk on disk and verify it is AES encrypted (not plaintext)
            first_chunk_id = file_info.chunks[0].chunk_id
            chunk_found_on_disk = False
            for nid in ["test-dn-1", "test-dn-2", "test-dn-3"]:
                cpath = os.path.join(TEST_DATA_DIR, nid, "chunks", f"{first_chunk_id}.enc")
                if os.path.exists(cpath):
                    chunk_found_on_disk = True
                    with open(cpath, "rb") as f:
                        disk_data = f.read()
                    # Disk data must NOT contain raw unencrypted bytes
                    assert disk_data[:100] != sample_bytes[:100]
                    # Attempt decrypt with our key - must succeed
                    decrypted = decrypt_block(disk_data, DEFAULT_AES_KEY.encode()[:32])
                    assert decrypted == sample_bytes[:chunk_size]

            assert chunk_found_on_disk, "Chunk file was not found on DataNode disks"

            # Download (get) and verify integrity
            with tempfile.NamedTemporaryFile("wb", delete=False) as out_tmp:
                out_path = out_tmp.name

            try:
                downloaded_bytes = self.client.get(remote_path, out_path)
                assert len(downloaded_bytes) == file_size
                download_sha = calculate_sha256(downloaded_bytes)
                assert download_sha == original_sha, "Downloaded file SHA-256 does not match original!"
            finally:
                if os.path.exists(out_path):
                    os.remove(out_path)

        finally:
            if os.path.exists(tmp_local_path):
                os.remove(tmp_local_path)

    def test_04_access_primitives_and_locking_rf3(self):
        """Tests RF3: cat, lock, unlock, file open/write/read."""
        # Write small text file
        text_content = "Distributed Systems 2026 - High Availability DFSha\nLine 2"
        with tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
            tmp.write(text_content.encode("utf-8"))
            tmp_path = tmp.name

        try:
            remote_txt = "/courses/notes.txt"
            self.client.put(tmp_path, remote_txt)

            # Test cat
            cat_out = self.client.cat(remote_txt)
            assert cat_out == text_content

            # Test lock
            assert self.client.lock(remote_txt) is True
            status = self.client.lock_status(remote_txt)
            assert status["is_locked"] is True
            assert status["locked_by"] == "admin"

            # Student1 attempts to overwrite or lock -> must fail
            student_client = DFShaClient(master_url=f"http://127.0.0.1:{MASTER_PORT}")
            student_client.login("student1", "telematica2026")
            with pytest.raises(Exception):
                student_client.lock(remote_txt)

            # Admin unlocks
            assert self.client.unlock(remote_txt) is True
            status_after = self.client.lock_status(remote_txt)
            assert status_after["is_locked"] is False

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_05_high_availability_datanode_failure_rf2_rnf2(self):
        """
        Tests RNF2: Fault tolerance under DataNode crash!
        We kill DataNode 1 (which holds chunks), and verify that get()
        still succeeds seamlessly using the replicas on DataNode 2 or 3!
        """
        remote_path = "/courses/distributed/project1_data.bin"
        stat_before = self.client.stat(remote_path)
        original_size = stat_before.size_bytes

        # Kill DataNode 1 (processes[2])
        dn1_proc = processes[2]
        dn1_proc.terminate()
        dn1_proc.wait(timeout=2.0)

        # Download file again - client must automatically failover to alive replicas!
        downloaded = self.client.get(remote_path)
        assert len(downloaded) == original_size, "Download failed after DataNode 1 crashed!"

    def test_06_high_availability_controlnode_standby_sync_rnf2(self):
        """
        Tests RNF2: Verifies that Standby ControlNode received metadata replication
        and has the complete filesystem namespace and user database.
        """
        standby_client = DFShaClient(master_url=f"http://127.0.0.1:{STANDBY_PORT}")
        standby_client.login("admin", "admin123")

        # Query directory in standby
        items = standby_client.ls("/courses/distributed")
        assert len(items) >= 1
        assert any(it.name == "project1_data.bin" for it in items)

        # Check cluster health of standby
        health = standby_client.cluster_health()
        assert health["role"] == "standby"
        assert health["is_primary"] is False

if __name__ == "__main__":
    pytest.main(["-v", __file__])
