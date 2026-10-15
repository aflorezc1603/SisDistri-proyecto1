#!/usr/bin/env bash
set -e

MODE=${1:-docker}

if [ "$MODE" = "docker" ]; then
    echo "Starting DFSha cluster via Docker Compose..."
    docker compose up --build -d
    echo "DFSha cluster started!"
    echo "ControlNode Primary: http://localhost:8000"
    echo "ControlNode Standby: http://localhost:8010"
    echo "DataNodes:           ports 8001, 8002, 8003"
    echo ""
    echo "To launch client CLI: docker compose run --rm client"
else
    echo "Starting DFSha cluster in native background processes..."
    PYTHONPATH=. python -m uvicorn controlnode.main:app --port 8000 &
    DFSHA_ROLE=standby DFSHA_PRIMARY_URL=http://localhost:8000 PYTHONPATH=. python -m uvicorn controlnode.main:app --port 8010 &
    DFSHA_NODE_ID=datanode-1 DFSHA_PORT=8001 DFSHA_PUBLIC_URL=http://localhost:8001 PYTHONPATH=. python -m uvicorn datanode.main:app --port 8001 &
    DFSHA_NODE_ID=datanode-2 DFSHA_PORT=8002 DFSHA_PUBLIC_URL=http://localhost:8002 PYTHONPATH=. python -m uvicorn datanode.main:app --port 8002 &
    DFSHA_NODE_ID=datanode-3 DFSHA_PORT=8003 DFSHA_PUBLIC_URL=http://localhost:8003 PYTHONPATH=. python -m uvicorn datanode.main:app --port 8003 &
    echo "Nodes started in background!"
fi
