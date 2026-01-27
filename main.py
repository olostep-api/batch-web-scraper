import argparse
import asyncio
import csv
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# If your class is in the same file, remove this import and use it directly.
from olostep import OlostepBatchClient


def _ts() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


async def run(
    csv_path: str,
    output_json_path: str,
    api_token: str,
    *,
    country: Optional[str] = None,
    parser_id: Optional[str] = None,
    poll_seconds: float = 5.0,
    retrieve_formats: Optional[List[str]] = None,
    log_every_n_polls: int = 1,
) -> None:
    if retrieve_formats is None:
        retrieve_formats = ["markdown"]

    # 1) Load CSV rows: expects columns: custom_id (or id), url
    items: List[Dict[str, str]] = []
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(
                "CSV has no header row. Expected columns: custom_id,url (or id,url)"
            )

        for row in reader:
            rid = (row.get("custom_id") or row.get("id") or "").strip()
            url = (row.get("url") or "").strip()
            if not rid or not url:
                continue
            items.append({"custom_id": rid, "url": url})

    if not items:
        raise ValueError(
            "No valid rows found. Ensure CSV has non-empty 'custom_id' (or 'id') and 'url' columns."
        )

    async with OlostepBatchClient(api_token=api_token) as client:
        # 2) Create batch
        batch_resp = await client.create_batch(
            items, country=country, parser_id=parser_id
        )
        batch_id = batch_resp.get("id")
        if not batch_id:
            raise RuntimeError(f"Batch create response missing 'id': {batch_resp}")

        print(f"[{_ts()}] Created batch: {batch_id} (urls={len(items)})")

        # 3) Poll until completed (log status + completed/total)
        poll_i = 0
        start = time.time()
        last_completed: Optional[int] = None
        last_total: Optional[int] = None

        while True:
            poll_i += 1
            progress = await client.get_batch_progress(batch_id)

            # Log every N polls OR when progress changes
            should_log = poll_i % max(1, log_every_n_polls) == 0
            changed = (progress.completed_urls != last_completed) or (
                progress.total_urls != last_total
            )
            if should_log or changed or progress.is_completed:
                elapsed = int(time.time() - start)
                print(
                    f"[{_ts()}] Batch {batch_id} status={progress.status} "
                    f"progress={progress.completed_urls}/{progress.total_urls} "
                    f"elapsed={elapsed}s"
                )
                last_completed = progress.completed_urls
                last_total = progress.total_urls

            if progress.is_completed:
                break

            await asyncio.sleep(poll_seconds)

        final_batch = await client.get_batch(batch_id)

        # 4) Fetch completed items + retrieve content, build output
        results: List[Dict[str, Any]] = []
        completed_count = 0

        async for item in client.iter_batch_items(batch_id, status="completed"):
            completed_count += 1
            retrieve_id = item.get("retrieve_id")
            custom_id = item.get("custom_id")
            url = item.get("url")

            if not retrieve_id:
                results.append(
                    {
                        "custom_id": custom_id,
                        "url": url,
                        "error": "missing_retrieve_id",
                    }
                )
                continue

            # optional small progress log while retrieving
            if completed_count % 50 == 0:
                print(
                    f"[{_ts()}] Retrieving content... {completed_count} completed items processed"
                )

            retrieved = await client.retrieve(retrieve_id, formats=retrieve_formats)  # type: ignore[arg-type]
            results.append(
                {
                    "custom_id": custom_id,
                    "url": url,
                    "retrieve_id": retrieve_id,
                    "retrieved": retrieved,
                }
            )

        failed_items: List[Dict[str, Any]] = []
        async for item in client.iter_batch_items(batch_id, status="failed"):
            failed_items.append(item)

        print(
            f"[{_ts()}] Items: completed={len(results)} failed={len(failed_items)} total={len(items)}"
        )

        payload = {
            "batch": final_batch,
            "batch_id": batch_id,
            "requested_count": len(items),
            "results_count": len(results),
            "results": results,
            "failed_count": len(failed_items),
            "failed_items": failed_items,
        }

        # 5) Save JSON
        with open(output_json_path, "w", encoding="utf-8") as out:
            json.dump(payload, out, ensure_ascii=False, indent=2)

        print(f"[{_ts()}] Saved: {output_json_path} (results={len(results)})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Olostep batch from a CSV and save results to JSON."
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV with columns: custom_id,url (or id,url)",
    )
    parser.add_argument("--out", required=True, help="Path to output JSON file")
    parser.add_argument(
        "--token",
        default=os.getenv("OLOSTEP_API_TOKEN"),
        help="Olostep API token (or set OLOSTEP_API_TOKEN)",
    )
    parser.add_argument(
        "--country", default=None, help="Optional country code (e.g., US, GB, PK)"
    )
    parser.add_argument(
        "--parser-id", default=None, help="Optional parser id for structured extraction"
    )
    parser.add_argument(
        "--poll-seconds", type=float, default=5.0, help="Polling interval seconds"
    )
    parser.add_argument(
        "--log-every", type=int, default=1, help="Log status every N polls (default: 1)"
    )
    parser.add_argument(
        "--formats",
        default="markdown",
        help='Comma-separated retrieve formats: "markdown,html,json"',
    )
    args = parser.parse_args()

    if not args.token:
        raise SystemExit("Missing API token. Pass --token or set OLOSTEP_API_TOKEN.")

    formats = [f.strip() for f in args.formats.split(",") if f.strip()]

    asyncio.run(
        run(
            csv_path=args.csv,
            output_json_path=args.out,
            api_token=args.token,
            country=args.country,
            parser_id=args.parser_id,
            poll_seconds=args.poll_seconds,
            retrieve_formats=formats,
            log_every_n_polls=args.log_every,
        )
    )


if __name__ == "__main__":
    main()
