"""
FINAL_STOP_SEC in main.py is a pure wall-clock deadline (compares
time.time() against the market's known end timestamp) that's meant to force
any open position closed 45 seconds before resolution, regardless of price.
If your machine's clock is meaningfully off from real UTC, that safety
margin silently shrinks or grows without anything telling you.

This doesn't need ntplib — it just compares your local clock against the
Date header of a plain HTTPS response (any well-run server's clock is
accurate to a small fraction of a second, far tighter than anything that
would matter here).

Usage:
    python check_clock_drift.py
"""
import time
from email.utils import parsedate_to_datetime
import requests


def main():
    urls = [
        "https://clob.polymarket.com",
        "https://www.google.com",
        "https://www.cloudflare.com",
    ]

    for url in urls:
        try:
            t0 = time.time()
            resp = requests.head(url, timeout=5)
            t1 = time.time()
        except Exception as e:
            print(f"{url}: request failed ({e})")
            continue

        date_header = resp.headers.get("Date")
        if not date_header:
            print(f"{url}: no Date header in response, skipping")
            continue

        server_time = parsedate_to_datetime(date_header).timestamp()
        # account for round-trip time by comparing against the midpoint
        local_time_at_response = (t0 + t1) / 2
        drift = local_time_at_response - server_time

        print(f"{url}:")
        print(f"  round-trip time : {t1 - t0:.3f}s")
        print(f"  estimated drift : {drift:+.3f}s  (positive = your clock is ahead)")

    print("\nIf drift is consistently more than ~1-2 seconds across these, "
          "sync your system clock (Windows: Settings -> Time & Language -> "
          "Sync now) before relying on FINAL_STOP_SEC as a hard safety "
          "margin.")


if __name__ == "__main__":
    main()