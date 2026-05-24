"""
keygen.py — CurveZMQ keypair generator
Run once. Copy output into Railway env vars and local .env file.
Never commit keys to repo.
"""

import base64
import zmq

def generate_keypair(name):
    public, secret = zmq.curve_keypair()
    return base64.b64encode(public).decode(), base64.b64encode(secret).decode()

def main():
    print("\n" + "="*60)
    print("CurveZMQ Keypair Generator — ftmo-bridge")
    print("="*60)

    print("\n--- RAILWAY (Publisher) keypair ---")
    rail_pub, rail_sec = generate_keypair("railway")
    print(f"RAILWAY_ZMQ_PUBLIC_KEY={rail_pub}")
    print(f"RAILWAY_ZMQ_SECRET_KEY={rail_sec}")

    print("\n--- BRIDGE (Subscriber) keypair ---")
    bridge_pub, bridge_sec = generate_keypair("bridge")
    print(f"BRIDGE_ZMQ_PUBLIC_KEY={bridge_pub}")
    print(f"BRIDGE_ZMQ_SECRET_KEY={bridge_sec}")

    print("\n--- .env block (paste into D:\\FTMO\\.env) ---")
    print(f"BRIDGE_ZMQ_PUBLIC_KEY={bridge_pub}")
    print(f"BRIDGE_ZMQ_SECRET_KEY={bridge_sec}")
    print(f"RAILWAY_ZMQ_PUBLIC_KEY={rail_pub}")

    print("\n--- Railway env vars block (paste into Railway dashboard) ---")
    print(f"RAILWAY_ZMQ_PUBLIC_KEY={rail_pub}")
    print(f"RAILWAY_ZMQ_SECRET_KEY={rail_sec}")
    print(f"BRIDGE_ZMQ_PUBLIC_KEY={bridge_pub}")

    print("\n--- Downstream port (Railway PUB → Bridge SUB) ---")
    print("ZMQ_DOWNSTREAM_PORT=5555")
    print("\n--- Upstream port (Bridge PUB → Railway SUB) ---")
    print("ZMQ_UPSTREAM_PORT=5556")

    print("\n" + "="*60)
    print("Store these in .env and Railway dashboard.")
    print("Never commit keys to git.")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()