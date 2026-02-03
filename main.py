import argparse
import asyncio
import csv
import json
import os
import time
import sys
from datetime import datetime, timezone
from loguru import logger
from typing import Any, Dict, List, Literal, Optional, Tuple, cast

from src.batch_scraper import BatchScraper

RetrieveFormat = Literal["html", "markdown", "json"]

def _ts() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _get_token(cli_token: Optional[str]) -> Optional[str]:
    return cli_token or os.getenv("OLOSTEP_API_TOKEN") or os.getenv("OLOSTEP_API_KEY")


def read_csv_items(csv_path: str) -> List[Dict[str, str]]:
    """
    Read CSV with headers and columns:
      - custom_id (or id)
      - url
    Returns list of {custom_id, url}.
    """
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

    return items


async def poll_until_completed(
    client: BatchScraper,
    batch_id: str,
    *,
    poll_seconds: float,
    log_every_n_polls: int,
) -> Dict[str, Any]:
    """
    Poll batch progress until status == completed.
    Returns the final batch object from GET /v1/batches/{batch_id}.
    """
    poll_i = 0
    start = time.time()
    last_completed: Optional[int] = None
    last_total: Optional[int] = None

    while True:
        poll_i += 1
        progress = await client.get_batch_progress(batch_id)

        should_log = poll_i % max(1, log_every_n_polls) == 0
        changed = (progress.completed_urls != last_completed) or (
            progress.total_urls != last_total
        )
        if should_log or changed or progress.is_completed:
            elapsed = int(time.time() - start)
            logger.info(
                f"[{_ts()}] Batch {batch_id} status={progress.status} "
                f"progress={progress.completed_urls}/{progress.total_urls} "
                f"elapsed={elapsed}s"
            )
            last_completed = progress.completed_urls
            last_total = progress.total_urls

        if progress.is_completed:
            break

        await asyncio.sleep(poll_seconds)

    return await client.get_batch(batch_id)


async def collect_results_and_failures(
    client: BatchScraper,
    batch_id: str,
    *,
    retrieve_formats: List[RetrieveFormat],
    items_limit: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Collect completed results (with retrieve content) and failed items.

    Also logs when Olostep returns hosted URLs instead of inline content
    (retrieve response contains size_exceeded=true and *_hosted_url fields).

    Returns (results, failed_items).
    """
    results: List[Dict[str, Any]] = []
    completed_count = 0

    # Track when Olostep returns hosted URLs instead of inline content
    size_exceeded_count = 0
    first_size_exceeded_ids: List[str] = []

    async for item in client.iter_batch_items(
        batch_id, status="completed", limit=items_limit
    ):
        completed_count += 1
        retrieve_id = item.get("retrieve_id")
        custom_id = item.get("custom_id")
        url = item.get("url")

        if not retrieve_id:
            results.append(
                {"custom_id": custom_id, "url": url, "error": "missing_retrieve_id"}
            )
            continue

        if completed_count % 50 == 0:
            logger.info(
                f"[{_ts()}] Retrieving content... {completed_count} completed items processed"
            )

        retrieved = await client.retrieve(retrieve_id, formats=retrieve_formats)

        # If content is too large, Olostep may return hosted URLs instead of inline content
        if isinstance(retrieved, dict) and retrieved.get("size_exceeded") is True:
            size_exceeded_count += 1
            if custom_id and len(first_size_exceeded_ids) < 3:
                first_size_exceeded_ids.append(str(custom_id))

        results.append(
            {
                "custom_id": custom_id,
                "url": url,
                "retrieve_id": retrieve_id,
                "retrieved": retrieved,
            }
        )

    if size_exceeded_count:
        id_hint = ""
        if first_size_exceeded_ids:
            id_hint = (
                f" (first affected custom_id(s): {', '.join(first_size_exceeded_ids)})"
            )

        logger.warning(
            f"[{_ts()}] Note: {size_exceeded_count} item(s) had size_exceeded=true{id_hint}. "
            "Their content may be in *_hosted_url fields (hosted URLs expire after ~7 days)."
        )

    failed_items: List[Dict[str, Any]] = []
    async for item in client.iter_batch_items(
        batch_id, status="failed", limit=items_limit
    ):
        failed_items.append(item)

    return results, failed_items


async def run(
    csv_path: str,
    output_json_path: str,
    api_token: str,
    *,
    country: Optional[str] = None,
    parser_id: Optional[str] = None,
    poll_seconds: float = 5.0,
    retrieve_formats: Optional[List[RetrieveFormat]] = None,
    log_every_n_polls: int = 1,
    items_limit: int = 50,
) -> None:
    """
    Run a batch from a CSV and write results to a JSON file.
    """
    if retrieve_formats is None:
        retrieve_formats = ["markdown"]

    items = read_csv_items(csv_path)

    async with BatchScraper(api_token=api_token) as client:
        batch_resp = await client.create_batch(
            items, country=country, parser_id=parser_id
        )
        batch_id = batch_resp.get("id")
        if not batch_id:
            raise RuntimeError(f"Batch create response missing 'id': {batch_resp}")

        logger.info(f"[{_ts()}] Created batch: {batch_id} (urls={len(items)})")

        final_batch = await poll_until_completed(
            client,
            batch_id,
            poll_seconds=poll_seconds,
            log_every_n_polls=log_every_n_polls,
        )

        results, failed_items = await collect_results_and_failures(
            client,
            batch_id,
            retrieve_formats=retrieve_formats,
            items_limit=items_limit,
        )

        logger.info(
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

        with open(output_json_path, "w", encoding="utf-8") as out:
            json.dump(payload, out, ensure_ascii=False, indent=2)

        logger.info(f"[{_ts()}] Saved: {output_json_path} (results={len(results)})")


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
        default=None,
        help="Olostep API token (or set OLOSTEP_API_TOKEN / OLOSTEP_API_KEY)",
    )
    parser.add_argument(
        "--country", default=None, help="Optional country code (e.g. US, GB, PK)"
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
    parser.add_argument(
        "--items-limit",
        type=int,
        default=50,
        help="Batch items page size (docs recommend 10-50). Default: 50",
    )
    args = parser.parse_args()

    token = _get_token(args.token)
    if not token:
        raise SystemExit(
            "Missing API token. Pass --token or set OLOSTEP_API_TOKEN / OLOSTEP_API_KEY."
        )

    allowed_formats = {"markdown", "html", "json"}
    formats_raw = [f.strip() for f in args.formats.split(",") if f.strip()]
    if invalid := [f for f in formats_raw if f not in allowed_formats]:
        raise SystemExit(
            f"Invalid --formats value(s): {', '.join(invalid)}. Allowed: markdown, html, json."
        )
    formats: List[RetrieveFormat] = [cast(RetrieveFormat, f) for f in formats_raw]

    if args.items_limit < 1:
        raise SystemExit("--items-limit must be >= 1")

    asyncio.run(
        run(
            csv_path=args.csv,
            output_json_path=args.out,
            api_token=token,
            country=args.country,
            parser_id=args.parser_id,
            poll_seconds=args.poll_seconds,
            retrieve_formats=formats,
            log_every_n_polls=args.log_every,
            items_limit=args.items_limit,
        )
    )


if __name__ == "__main__":
    main()
