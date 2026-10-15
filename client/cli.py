import argparse
import os
import sys
import shlex
import time
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from client.sdk import DFShaClient

console = Console()

def format_size(size_bytes: int) -> str:
    """Formats bytes to human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def format_time(ts: float) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def render_ls_table(items: list, current_path: str):
    table = Table(title=f"Directory: [bold cyan]{current_path}[/bold cyan]", show_header=True, header_style="bold magenta")
    table.add_column("Type", width=4, justify="center")
    table.add_column("Permissions", width=11)
    table.add_column("Owner", width=10)
    table.add_column("Size", width=12, justify="right")
    table.add_column("Chunks", width=8, justify="right")
    table.add_column("Lock", width=8, justify="center")
    table.add_column("Modified", width=20)
    table.add_column("Name", style="bold")

    for it in items:
        ftype = "DIR" if it.is_directory else "FILE"
        color = "cyan" if it.is_directory else "green"
        lock_str = f"[red]LOCKED ({it.locked_by})[/red]" if it.is_locked else "[green]NO[/green]"
        chunks_count = str(len(it.chunks)) if not it.is_directory else "-"

        table.add_row(
            f"[{color}]{ftype}[/{color}]",
            it.permissions,
            it.owner,
            "-" if it.is_directory else format_size(it.size_bytes),
            chunks_count,
            lock_str,
            format_time(it.modified_at),
            f"[{color}]{it.name}[/{color}]"
        )
    console.print(table)

def render_nodes_table(nodes: list):
    table = Table(title="[bold yellow]DFSha Cluster - Active DataNodes[/bold yellow]", show_header=True, header_style="bold blue")
    table.add_column("Node ID", width=16)
    table.add_column("Address / URL", width=28)
    table.add_column("Status", width=10, justify="center")
    table.add_column("Available Space", width=18, justify="right")
    table.add_column("Last Heartbeat", width=16, justify="right")

    now = time.time()
    for n in nodes:
        status_color = "bold green" if n.is_alive else "bold red"
        status_txt = "ALIVE" if n.is_alive else "DEAD"
        time_ago = f"{int(now - n.last_heartbeat)}s ago" if n.last_heartbeat else "never"
        table.add_row(
            n.node_id,
            n.url,
            f"[{status_color}]{status_txt}[/{status_color}]",
            format_size(n.available_space),
            time_ago
        )
    console.print(table)

def print_help():
    panel_content = """
[bold cyan]File System Operations (RF1):[/bold cyan]
  [green]ls [path][/green]                 List files and directories
  [green]cd <path>[/green]                 Change working directory
  [green]pwd[/green]                       Print working directory
  [green]mkdir <path>[/green]              Create directory
  [green]rmdir <path>[/green]              Remove empty directory
  [green]rm [-r] <path>[/green]            Delete file or directory (recursive with -r)

[bold cyan]Transfer Operations (RF2):[/bold cyan]
  [green]put <local> [remote][/green]       Upload file, partition in chunks, distribute to DataNodes (alias: send)
  [green]get <remote> [local][/green]       Download file, reassemble chunks with replica failover (alias: recv)

[bold cyan]Access & Concurrency Primitives (RF3):[/bold cyan]
  [green]cat <remote_file>[/green]          View file content
  [green]stat <remote_file>[/green]         View detailed metadata & chunk mapping
  [green]lock <remote_file>[/green]         Acquire exclusive lock on file
  [green]unlock <remote_file>[/green]       Release file lock

[bold cyan]Cluster & Security (RNF2 & RNF6):[/bold cyan]
  [green]login <user> <pass>[/green]       Authenticate and receive JWT token
  [green]register <user> <pass>[/green]    Create new user account
  [green]whoami[/green]                    Display logged in user info
  [green]nodes[/green]                     List cluster DataNodes and health status
  [green]cluster[/green]                   Show ControlNode primary/standby health status

[bold cyan]General:[/bold cyan]
  [green]help[/green]                      Show this help menu
  [green]exit[/green] / [green]quit[/green]              Exit DFSha CLI shell
"""
    console.print(Panel(panel_content, title="[bold green]DFSha CLI - Command Reference[/bold green]", border_style="cyan"))

def main():
    parser = argparse.ArgumentParser(description="DFSha Interactive CLI and Client Tool")
    parser.add_argument("--master", default=os.getenv("DFSHA_CONTROLNODE_URL", "http://localhost:8000"), help="ControlNode URL")
    parser.add_argument("--standby", default=os.getenv("DFSHA_CONTROLNODE_STANDBY_URL", ""), help="Standby ControlNode URL")
    parser.add_argument("--user", default="admin", help="Default username")
    parser.add_argument("--password", default="admin123", help="Default password")
    parser.add_argument("command", nargs="*", help="Optional non-interactive command to run")

    args = parser.parse_args()

    client = DFShaClient(master_url=args.master, standby_url=args.standby)

    # Auto-login with defaults if provided
    try:
        client.login(args.user, args.password)
    except Exception:
        pass

    # If commands passed as args, execute and exit
    if args.command:
        cmd = args.command[0].lower()
        cmd_args = args.command[1:]
        try:
            execute_cmd(client, cmd, cmd_args)
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}")
            sys.exit(1)
        sys.exit(0)

    # Interactive Shell Loop
    console.print(Panel(
        "[bold cyan]Welcome to DFSha Distributed File System CLI[/bold cyan]\n"
        "[italic white]High Availability (HA), Performance, Transparency and Security[/italic white]\n"
        "Type [bold yellow]help[/bold yellow] for available commands or [bold yellow]exit[/bold yellow] to quit.",
        border_style="bold green"
    ))

    while True:
        try:
            user_label = client.username or "anonymous"
            prompt = f"[bold green]{user_label}[/bold green]@[bold blue]dfsha[/bold blue]:[bold yellow]{client.pwd()}[/bold yellow]$ "
            line = console.input(prompt).strip()
            if not line:
                continue

            parts = shlex.split(line)
            cmd = parts[0].lower()
            cmd_args = parts[1:]

            if cmd in ("exit", "quit", "q"):
                console.print("[yellow]Exiting DFSha CLI. Goodbye![/yellow]")
                break
            elif cmd == "help":
                print_help()
            else:
                execute_cmd(client, cmd, cmd_args)

        except KeyboardInterrupt:
            console.print("\n[yellow]KeyboardInterrupt. Type 'exit' to quit.[/yellow]")
        except EOFError:
            break
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}")

def execute_cmd(client: DFShaClient, cmd: str, args: list):
    if cmd == "login":
        if len(args) < 2:
            console.print("[red]Usage: login <username> <password>[/red]")
            return
        client.login(args[0], args[1])
        console.print(f"[green]Successfully logged in as '{args[0]}'.[/green]")

    elif cmd == "register":
        if len(args) < 2:
            console.print("[red]Usage: register <username> <password> [role][/red]")
            return
        role = args[2] if len(args) > 2 else "user"
        client.register(args[0], args[1], role)
        console.print(f"[green]User '{args[0]}' registered successfully.[/green]")

    elif cmd == "whoami":
        me = client.whoami()
        console.print(f"[cyan]Logged in as:[/cyan] [bold]{me['username']}[/bold] (Role: {me['role']})")

    elif cmd == "pwd":
        console.print(client.pwd())

    elif cmd == "cd":
        target = args[0] if args else "/"
        client.cd(target)

    elif cmd == "ls":
        target = args[0] if args else None
        items = client.ls(target)
        render_ls_table(items, client._resolve_path(target))

    elif cmd == "mkdir":
        if not args:
            console.print("[red]Usage: mkdir <path>[/red]")
            return
        client.mkdir(args[0])
        console.print(f"[green]Directory '{args[0]}' created.[/green]")

    elif cmd == "rmdir":
        if not args:
            console.print("[red]Usage: rmdir <path>[/red]")
            return
        client.rmdir(args[0])
        console.print(f"[green]Directory '{args[0]}' removed.[/green]")

    elif cmd == "rm":
        if not args:
            console.print("[red]Usage: rm [-r] <path>[/red]")
            return
        recursive = False
        target = args[0]
        if args[0] == "-r" and len(args) > 1:
            recursive = True
            target = args[1]
        client.rm(target, recursive=recursive)
        console.print(f"[green]Successfully removed '{target}'.[/green]")

    elif cmd in ("put", "send"):
        if not args:
            console.print("[red]Usage: put <local_file> [remote_path][/red]")
            return
        local_f = args[0]
        remote_f = args[1] if len(args) > 1 else None
        t0 = time.time()
        info = client.put(local_f, remote_f)
        elapsed = time.time() - t0
        console.print(f"[green]Uploaded '{info.path}' ({format_size(info.size_bytes)}) in {len(info.chunks)} chunks in {elapsed:.2f}s.[/green]")

    elif cmd in ("get", "recv", "receive"):
        if not args:
            console.print("[red]Usage: get <remote_path> [local_path][/red]")
            return
        remote_f = args[0]
        local_f = args[1] if len(args) > 1 else None
        t0 = time.time()
        content = client.get(remote_f, local_f)
        elapsed = time.time() - t0
        console.print(f"[green]Downloaded '{remote_f}' ({format_size(len(content))}) in {elapsed:.2f}s.[/green]")

    elif cmd == "cat":
        if not args:
            console.print("[red]Usage: cat <remote_path>[/red]")
            return
        out = client.cat(args[0])
        console.print(out)

    elif cmd == "stat":
        if not args:
            console.print("[red]Usage: stat <remote_path>[/red]")
            return
        info = client.stat(args[0])
        console.print(Panel(
            f"[bold]Path:[/bold] {info.path}\n"
            f"[bold]Type:[/bold] {'Directory' if info.is_directory else 'File'}\n"
            f"[bold]Size:[/bold] {format_size(info.size_bytes)} ({info.size_bytes} bytes)\n"
            f"[bold]Owner:[/bold] {info.owner}\n"
            f"[bold]Permissions:[/bold] {info.permissions}\n"
            f"[bold]Created:[/bold] {format_time(info.created_at)}\n"
            f"[bold]Modified:[/bold] {format_time(info.modified_at)}\n"
            f"[bold]Locked:[/bold] {info.is_locked} (by: {info.locked_by})\n"
            f"[bold]Total Chunks:[/bold] {len(info.chunks)}\n"
            + "\n".join([f"  Chunk {c.chunk_index} ({c.chunk_id}): {format_size(c.size_bytes)}, SHA256={c.sha256[:12]}..., Nodes={c.locations}" for c in info.chunks]),
            title=f"Metadata: [bold cyan]{info.name}[/bold cyan]",
            border_style="magenta"
        ))

    elif cmd == "lock":
        if not args:
            console.print("[red]Usage: lock <remote_path>[/red]")
            return
        client.lock(args[0])
        console.print(f"[green]Lock acquired on '{args[0]}'.[/green]")

    elif cmd == "unlock":
        if not args:
            console.print("[red]Usage: unlock <remote_path>[/red]")
            return
        client.unlock(args[0])
        console.print(f"[green]Lock released on '{args[0]}'.[/green]")

    elif cmd == "nodes":
        nodes = client.list_nodes()
        render_nodes_table(nodes)

    elif cmd == "cluster":
        health = client.cluster_health()
        console.print(Panel(
            f"[bold]Role:[/bold] {health.get('role', 'unknown').upper()}\n"
            f"[bold]Is Primary:[/bold] {health.get('is_primary')}\n"
            f"[bold]Active DataNodes:[/bold] {health.get('alive_datanodes')}\n"
            f"[bold]Files in Namespace:[/bold] {health.get('total_files')}\n"
            f"[bold]Status:[/bold] {health.get('status')}",
            title="[bold yellow]Cluster Health & HA Status[/bold yellow]",
            border_style="yellow"
        ))

    else:
        console.print(f"[red]Unknown command: '{cmd}'. Type 'help' for command list.[/red]")

if __name__ == "__main__":
    main()
