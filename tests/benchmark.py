import os
import sys
import time
import tempfile
from rich.console import Console
from rich.table import Table

from client.sdk import DFShaClient

console = Console()

def run_benchmark():
    master_url = os.getenv("DFSHA_CONTROLNODE_URL", "http://localhost:8000")
    client = DFShaClient(master_url=master_url)
    try:
        client.login("admin", "admin123")
    except Exception as e:
        console.print(f"[red]Failed to connect/login to ControlNode at {master_url}: {e}[/red]")
        return

    sizes_mb = [1, 5, 10]
    table = Table(title="[bold green]DFSha Performance & Scalability Benchmark (RNF5)[/bold green]")
    table.add_column("File Size", justify="right", style="cyan")
    table.add_column("Chunks", justify="center")
    table.add_column("Upload Time", justify="right")
    table.add_column("Upload Speed", justify="right", style="bold green")
    table.add_column("Download Time", justify="right")
    table.add_column("Download Speed", justify="right", style="bold blue")

    client.mkdir("/benchmarks")

    for sz in sizes_mb:
        num_bytes = sz * 1024 * 1024
        data = os.urandom(num_bytes)
        with tempfile.NamedTemporaryFile("wb", delete=False) as f:
            f.write(data)
            tmp_local = f.name

        remote_path = f"/benchmarks/test_{sz}mb.bin"

        try:
            # Upload Benchmark
            t0 = time.time()
            info = client.put(tmp_local, remote_path, chunk_size=1024*1024, replication_factor=2)
            t_up = time.time() - t0
            up_speed = (sz / t_up) if t_up > 0 else 0

            # Download Benchmark
            t1 = time.time()
            recv_data = client.get(remote_path)
            t_down = time.time() - t1
            down_speed = (sz / t_down) if t_down > 0 else 0

            assert len(recv_data) == num_bytes

            table.add_row(
                f"{sz} MB",
                str(len(info.chunks)),
                f"{t_up:.2f} s",
                f"{up_speed:.2f} MB/s",
                f"{t_down:.2f} s",
                f"{down_speed:.2f} MB/s"
            )
        finally:
            if os.path.exists(tmp_local):
                os.remove(tmp_local)

    console.print(table)

if __name__ == "__main__":
    run_benchmark()
