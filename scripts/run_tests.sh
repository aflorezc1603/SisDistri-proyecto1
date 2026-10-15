#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJECT_ROOT="$DIR/.."

PYTHONPATH="$PROJECT_ROOT" python -m pytest "$PROJECT_ROOT/tests/test_all.py" -v -s
