# scripts/refresh_upstox_token.py

import os
import sys
import requests

"""
Reads:
  UPSTOX_CLIENT_ID
  UPSTOX_CLIENT_SECRET
  UPSTOX_REFRESH_TOKEN

Prints:
  <access_token>   (only the token, to stdout)
"""

API_URL = "https://api.upstox.com/v2/login/authorization/token"


def main():
    client_id = os.environ.get("ea83f8be-6d2c-4c0d-b18a-8f58f40d9019")
    client_secret = os.environ.get("xnzua69v13")
    refresh_token = os.environ.get("UeyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzODEyNDkiLCJqdGkiOiI2OTJiOGQ2ZWJhYzQ4MDMwYmFiODMyZDYiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc2NDQ2MTkzNCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzY0NTQwMDAwfQ.rajWiT1m0iHRSvj1FZ5Hr7Zs5M8w4Y5u3yUYetCoqzs")

    if not client_id or not client_secret or not refresh_token:
        print("Missing one of UPSTOX_CLIENT_ID / UPSTOX_CLIENT_SECRET / UPSTOX_REFRESH_TOKEN",
              file=sys.stderr)
        sys.exit(1)

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    resp = requests.post(API_URL, data=payload, timeout=30)
    if resp.status_code != 200:
        print(f"Refresh failed: HTTP {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    access_token = data.get("access_token")
    if not access_token:
        print(f"No access_token in response: {data}", file=sys.stderr)
        sys.exit(1)

    # Print ONLY the token, so workflow can capture it easily
    print(access_token)


if __name__ == "__main__":
    main()
