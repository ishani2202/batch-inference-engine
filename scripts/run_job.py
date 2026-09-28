"""Tiny client: create a job, print progress until it finishes, save the results.

    python scripts/run_job.py                          # data/sample_batch.json
    python scripts/run_job.py --input big_batch.json --no-download
"""

import argparse
import json
import sys
import time

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", default="http://127.0.0.1:8000")
    parser.add_argument("--input", default="sample_batch.json", help="file name inside DATA_DIR")
    parser.add_argument("--webhook-url")
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between status polls")
    parser.add_argument("--no-download", action="store_true")
    args = parser.parse_args()

    with httpx.Client(base_url=args.server, timeout=30) as http:
        body = {"input_file": args.input}
        if args.webhook_url:
            body["webhook_url"] = args.webhook_url
        resp = http.post("/job", json=body)
        if resp.status_code != 202:
            sys.exit(f"create failed: {resp.status_code} {resp.text}")
        job_id = resp.json()["job_id"]
        print(f"job {job_id} created")

        while True:
            s = http.get(f"/job/{job_id}/status").json()
            p, perf = s["progress"], s["performance"]
            print(
                f"[{s['status']:>9}] {p['done']}/{p['total']} ({p['percent']}%)  ok={p['succeeded']} "
                f"failed={p['failed']}  retries={perf['retries']} 429s={perf['rate_limited_429s']}  "
                f"limit={perf['concurrency_limit']}  {perf['items_per_second']} items/s",
                flush=True,
            )
            if s["status"] in ("completed", "failed"):
                break
            time.sleep(args.interval)

        print(json.dumps(s, indent=2))
        if not args.no_download:
            for kind in ("results", "errors"):
                path = f"{job_id}-{kind}.json"
                with http.stream("GET", f"/job/{job_id}/download", params={"kind": kind}) as r, open(path, "wb") as f:
                    for chunk in r.iter_bytes():
                        f.write(chunk)
                print(f"saved {path}")


if __name__ == "__main__":
    main()
