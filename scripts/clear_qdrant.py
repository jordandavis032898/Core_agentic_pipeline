#!/usr/bin/env python3
"""
Clear all collections in Qdrant (for a fresh test run).
Usage (from agentic-router directory):
  python scripts/clear_qdrant.py
  ENV_FILE=.env python scripts/clear_qdrant.py
"""
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
env_file = os.getenv("ENV_FILE", ".env")
env_path = root / env_file
if not env_path.exists():
    env_path = root / ".env.prod"
if env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path)
    except ImportError:
        pass

sys.path.insert(0, str(root))
sys.path.insert(0, str(root / "pipeline_v1_final"))

from helper import get_qdrant_client


def main():
    print("Connecting to Qdrant...")
    try:
        client = get_qdrant_client()
        collections = client.get_collections()
        names = [c.name for c in collections.collections]
        if not names:
            print("  No collections found. Qdrant is already empty.")
            return
        print(f"  Found {len(names)} collection(s): {names}")
        for name in names:
            client.delete_collection(name)
            print(f"  Deleted: {name}")
        print("  Done. Qdrant is clear.")
    except Exception as e:
        print(f"  Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
