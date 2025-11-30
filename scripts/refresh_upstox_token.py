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
    client_id = os.environ.get("UPSTOX_CLIENT_ID")
    client_secret = os.environ.get("UPSTOX_CLIENT_SECRET")
    refresh_token = os.environ.get("UPSTOX_REFRESH_TOKEN")

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
