from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, Sequence, Tuple, Union, cast
from urllib.parse import urlparse

from loguru import logger

from .batch_scraper import BatchScraper

RetrieveFormat = Literal["html", "markdown", "json"]
ProgressCallback = Callable[[Dict[str, Any]], None]

ALLOWED_RETRIEVE_FORMATS: Tuple[RetrieveFormat, ...] = ("markdown", "html", "json")
REQUIRED_OUTPUT_KEYS = (
    "batch",
    "batch_id",
    "failed_count",
    "failed_items",
    "requested_count",
    "results",
    "results_count",
)


@dataclass(frozen=True)
class BatchResultMetrics:
    requested_count: int
    completed_count: int
    failed_count: int
    retrieve_success_count: int
    hosted_url_count: int
    size_exceeded_count: int
    partial_success: bool
    batch_status: str
    available_formats: List[RetrieveFormat]


@dataclass(frozen=True)
class OutputPayload:
    batch: Dict[str, Any]
    batch_id: str
    requested_count: int
    results_count: int
    results: List[Dict[str, Any]]
    failed_count: int
    failed_items: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch": self.batch,
            "batch_id": self.batch_id,
            "requested_count": self.requested_count,
            "results_count": self.results_count,
            "results": self.results,
            "failed_count": self.failed_count,
            "failed_items": self.failed_items,
        }


def _ts() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _emit_progress(callback: Optional[ProgressCallback], **event: Any) -> None:
    if callback is None:
        return
    callback(event)


def _read_env_file(env_path: Union[str, Path]) -> Dict[str, str]:
    path = Path(env_path)
    if not path.is_file():
        return {}

    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        cleaned = value.strip().strip('"').strip("'")
        values[key.strip()] = cleaned
    return values


def get_api_token(
    cli_token: Optional[str],
    *,
    env_path: Union[str, Path] = ".env",
) -> Optional[str]:
    token = (cli_token or "").strip()
    if token:
        return token

    for env_key in ("OLOSTEP_API_TOKEN", "OLOSTEP_API_KEY"):
        env_value = (os.getenv(env_key) or "").strip()
        if env_value:
            return env_value

    env_values = _read_env_file(env_path)
    for env_key in ("OLOSTEP_API_TOKEN", "OLOSTEP_API_KEY"):
        env_value = (env_values.get(env_key) or "").strip()
        if env_value:
            return env_value

    return None


def parse_retrieve_formats(
    raw_formats: Optional[Union[str, Sequence[str]]],
) -> List[RetrieveFormat]:
    if raw_formats is None:
        parts = ["markdown"]
    elif isinstance(raw_formats, str):
        parts = [part.strip().lower() for part in raw_formats.split(",") if part.strip()]
    else:
        parts = [str(part).strip().lower() for part in raw_formats if str(part).strip()]

    if not parts:
        parts = ["markdown"]

    invalid = [part for part in parts if part not in ALLOWED_RETRIEVE_FORMATS]
    if invalid:
        raise ValueError(
            "Invalid formats: "
            f"{', '.join(invalid)}. Allowed: {', '.join(ALLOWED_RETRIEVE_FORMATS)}."
        )

    unique_parts: List[RetrieveFormat] = []
    seen = set()
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        unique_parts.append(cast(RetrieveFormat, part))
    return unique_parts


def _parse_csv_rows(reader: csv.DictReader[str]) -> List[Dict[str, str]]:
    if not reader.fieldnames:
        raise ValueError(
            "CSV has no header row. Expected columns: custom_id,url (or id,url)."
        )

    items: List[Dict[str, str]] = []
    for row in reader:
        record_id = (row.get("custom_id") or row.get("id") or "").strip()
        url = (row.get("url") or "").strip()
        if not record_id or not url:
            continue
        items.append({"custom_id": record_id, "url": url})

    if not items:
        raise ValueError(
            "No valid rows found. Ensure CSV has non-empty 'custom_id' (or 'id') and 'url' columns."
        )

    return items


def read_csv_items(csv_path: Union[str, Path]) -> List[Dict[str, str]]:
    return read_csv_items_from_text(Path(csv_path).read_text(encoding="utf-8-sig"))


def read_csv_items_from_text(text: str) -> List[Dict[str, str]]:
    reader = csv.DictReader(io.StringIO(text))
    return _parse_csv_rows(reader)


def read_csv_items_from_bytes(raw_bytes: bytes) -> List[Dict[str, str]]:
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("CSV must be UTF-8 encoded.") from exc
    return read_csv_items_from_text(text)


async def poll_until_completed(
    client: BatchScraper,
    batch_id: str,
    *,
    poll_seconds: float,
    log_every_n_polls: int,
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict[str, Any]:
    poll_i = 0
    start = time.time()
    last_completed: Optional[int] = None
    last_total: Optional[int] = None

    while True:
        poll_i += 1
        progress = await client.get_batch_progress(batch_id)
        elapsed = int(time.time() - start)

        should_log = poll_i % max(1, log_every_n_polls) == 0
        changed = (progress.completed_urls != last_completed) or (
            progress.total_urls != last_total
        )
        if should_log or changed or progress.is_completed:
            logger.info(
                f"[{_ts()}] Batch {batch_id} status={progress.status} "
                f"progress={progress.completed_urls}/{progress.total_urls} "
                f"elapsed={elapsed}s"
            )
            last_completed = progress.completed_urls
            last_total = progress.total_urls

        _emit_progress(
            progress_callback,
            phase="polling",
            batch_id=batch_id,
            status=progress.status,
            completed_urls=progress.completed_urls,
            total_urls=progress.total_urls,
            elapsed_seconds=elapsed,
            message=(
                f"Batch status: {progress.status}. "
                f"Processed {progress.completed_urls} of {progress.total_urls} URLs."
            ),
        )

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
    expected_results: Optional[int] = None,
    progress_callback: Optional[ProgressCallback] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    results: List[Dict[str, Any]] = []
    completed_count = 0
    size_exceeded_count = 0
    first_size_exceeded_ids: List[str] = []

    _emit_progress(
        progress_callback,
        phase="retrieving",
        batch_id=batch_id,
        processed_results=0,
        expected_results=expected_results or 0,
        message="Retrieving content for completed items.",
    )

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
            _emit_progress(
                progress_callback,
                phase="retrieving",
                batch_id=batch_id,
                processed_results=completed_count,
                expected_results=expected_results or completed_count,
                current_custom_id=custom_id,
                current_url=url,
                message=(
                    f"Skipped item {custom_id or url or completed_count}: missing retrieve_id."
                ),
            )
            continue

        if completed_count % 50 == 0:
            logger.info(
                f"[{_ts()}] Retrieving content... {completed_count} completed items processed"
            )

        retrieved = await client.retrieve(retrieve_id, formats=retrieve_formats)

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

        _emit_progress(
            progress_callback,
            phase="retrieving",
            batch_id=batch_id,
            processed_results=completed_count,
            expected_results=expected_results or completed_count,
            current_custom_id=custom_id,
            current_url=url,
            retrieve_id=retrieve_id,
            message=(
                f"Retrieved {completed_count} of {expected_results or completed_count} "
                "completed items."
            ),
        )

    if size_exceeded_count:
        id_hint = ""
        if first_size_exceeded_ids:
            id_hint = f" (first affected custom_id(s): {', '.join(first_size_exceeded_ids)})"

        logger.warning(
            f"[{_ts()}] Note: {size_exceeded_count} item(s) had size_exceeded=true{id_hint}. "
            "Their content may be in *_hosted_url fields (hosted URLs expire after ~7 days)."
        )

    failed_items: List[Dict[str, Any]] = []
    async for item in client.iter_batch_items(
        batch_id, status="failed", limit=items_limit
    ):
        failed_items.append(item)

    _emit_progress(
        progress_callback,
        phase="finalizing",
        batch_id=batch_id,
        results_count=len(results),
        failed_count=len(failed_items),
        message="Collected completed and failed items.",
    )

    return results, failed_items


def build_output_payload(
    *,
    final_batch: Dict[str, Any],
    batch_id: str,
    requested_count: int,
    results: List[Dict[str, Any]],
    failed_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return OutputPayload(
        batch=final_batch,
        batch_id=batch_id,
        requested_count=requested_count,
        results_count=len(results),
        results=results,
        failed_count=len(failed_items),
        failed_items=failed_items,
    ).to_dict()


async def run_batch(
    items: Sequence[Dict[str, str]],
    api_token: str,
    *,
    country: Optional[str] = None,
    parser_id: Optional[str] = None,
    poll_seconds: float = 5.0,
    retrieve_formats: Optional[Sequence[RetrieveFormat]] = None,
    log_every_n_polls: int = 1,
    items_limit: int = 50,
    base_url: str = "https://api.olostep.com",
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict[str, Any]:
    normalized_formats = parse_retrieve_formats(retrieve_formats)

    if items_limit < 1:
        raise ValueError("items_limit must be >= 1")

    try:
        async with BatchScraper(api_token=api_token, base_url=base_url) as client:
            batch_resp = await client.create_batch(
                items,
                country=country,
                parser_id=parser_id,
            )
            batch_id = str(batch_resp.get("id") or "").strip()
            if not batch_id:
                raise RuntimeError(f"Batch create response missing 'id': {batch_resp}")

            logger.info(f"[{_ts()}] Created batch: {batch_id} (urls={len(items)})")
            _emit_progress(
                progress_callback,
                phase="batch_created",
                batch_id=batch_id,
                total_urls=len(items),
                completed_urls=0,
                status="created",
                message=f"Created batch {batch_id} for {len(items)} URL(s).",
            )

            final_batch = await poll_until_completed(
                client,
                batch_id,
                poll_seconds=poll_seconds,
                log_every_n_polls=log_every_n_polls,
                progress_callback=progress_callback,
            )

            expected_results = int(final_batch.get("completed_urls") or 0)
            results, failed_items = await collect_results_and_failures(
                client,
                batch_id,
                retrieve_formats=normalized_formats,
                items_limit=items_limit,
                expected_results=expected_results,
                progress_callback=progress_callback,
            )

            logger.info(
                f"[{_ts()}] Items: completed={len(results)} failed={len(failed_items)} total={len(items)}"
            )

            payload = build_output_payload(
                final_batch=final_batch,
                batch_id=batch_id,
                requested_count=len(items),
                results=results,
                failed_items=failed_items,
            )

            _emit_progress(
                progress_callback,
                phase="completed",
                batch_id=batch_id,
                results_count=len(results),
                failed_count=len(failed_items),
                requested_count=len(items),
                message=(
                    f"Batch complete. Retrieved {len(results)} item(s) with "
                    f"{len(failed_items)} failure(s)."
                ),
            )
            return payload
    except Exception as exc:
        _emit_progress(
            progress_callback,
            phase="error",
            status="error",
            message=str(exc),
        )
        raise


def run_batch_sync(
    items: Sequence[Dict[str, str]],
    api_token: str,
    **kwargs: Any,
) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run_batch(items, api_token, **kwargs))
    raise RuntimeError("run_batch_sync cannot be used from an active event loop.")


def save_output_payload(payload: Mapping[str, Any], output_json_path: Union[str, Path]) -> None:
    validated = validate_saved_output(payload)
    Path(output_json_path).write_text(
        json.dumps(validated, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def validate_saved_output(payload: Mapping[str, Any]) -> Dict[str, Any]:
    missing = [key for key in REQUIRED_OUTPUT_KEYS if key not in payload]
    if missing:
        raise ValueError(f"Output payload is missing key(s): {', '.join(missing)}")

    batch = payload["batch"]
    if not isinstance(batch, Mapping):
        raise ValueError("Output payload field 'batch' must be an object.")

    results = payload["results"]
    failed_items = payload["failed_items"]
    if not isinstance(results, list):
        raise ValueError("Output payload field 'results' must be a list.")
    if not isinstance(failed_items, list):
        raise ValueError("Output payload field 'failed_items' must be a list.")

    batch_id = str(payload["batch_id"]).strip()
    if not batch_id:
        raise ValueError("Output payload field 'batch_id' must be a non-empty string.")

    normalized = dict(payload)
    normalized["batch"] = dict(batch)
    normalized["results"] = list(results)
    normalized["failed_items"] = list(failed_items)
    for count_key in ("requested_count", "results_count", "failed_count"):
        try:
            normalized[count_key] = int(payload[count_key])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Output payload field '{count_key}' must be an integer."
            ) from exc

    normalized["batch_id"] = batch_id
    return normalized


def load_saved_output(raw_payload: Union[str, bytes]) -> Dict[str, Any]:
    if isinstance(raw_payload, bytes):
        try:
            raw_payload = raw_payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("Saved output must be UTF-8 encoded JSON.") from exc

    try:
        parsed = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError("Saved output is not valid JSON.") from exc

    if not isinstance(parsed, Mapping):
        raise ValueError("Saved output must be a JSON object.")
    return validate_saved_output(parsed)


def _content_to_text(content: Any) -> Optional[str]:
    if content is None:
        return None
    if isinstance(content, str):
        return content
    return json.dumps(content, ensure_ascii=False, indent=2)


def build_content_preview(retrieved: Mapping[str, Any], *, max_chars: int = 180) -> str:
    for format_name in ALLOWED_RETRIEVE_FORMATS:
        text = _content_to_text(retrieved.get(f"{format_name}_content"))
        if not text:
            continue
        compact = " ".join(text.split())
        if len(compact) > max_chars:
            return compact[: max_chars - 1].rstrip() + "..."
        return compact

    hosted_formats = [
        format_name.title()
        for format_name in ALLOWED_RETRIEVE_FORMATS
        if retrieved.get(f"{format_name}_hosted_url")
    ]
    if hosted_formats:
        return f"Hosted content available for {', '.join(hosted_formats)}."
    return "No inline content returned."


def _available_format_ids(retrieved: Mapping[str, Any]) -> List[RetrieveFormat]:
    available: List[RetrieveFormat] = []
    for format_name in ALLOWED_RETRIEVE_FORMATS:
        if retrieved.get(f"{format_name}_content") is not None or retrieved.get(
            f"{format_name}_hosted_url"
        ):
            available.append(format_name)
    return available


def _hosted_format_ids(retrieved: Mapping[str, Any]) -> List[RetrieveFormat]:
    hosted: List[RetrieveFormat] = []
    for format_name in ALLOWED_RETRIEVE_FORMATS:
        if retrieved.get(f"{format_name}_hosted_url"):
            hosted.append(format_name)
    return hosted


def _domain_for_url(url: str) -> str:
    parsed = urlparse(url)
    domain = parsed.netloc or ""
    if domain.startswith("www."):
        return domain[4:]
    return domain


def derive_result_metrics(payload: Mapping[str, Any]) -> BatchResultMetrics:
    validated = validate_saved_output(payload)
    results = validated["results"]

    retrieve_success_count = 0
    hosted_url_count = 0
    size_exceeded_count = 0
    available_formats: List[RetrieveFormat] = []
    seen_formats = set()

    for result in results:
        retrieved = result.get("retrieved") or {}
        if not isinstance(retrieved, Mapping):
            continue
        if retrieved.get("success") is True:
            retrieve_success_count += 1
        if retrieved.get("size_exceeded") is True:
            size_exceeded_count += 1

        hosted_formats = _hosted_format_ids(retrieved)
        if hosted_formats:
            hosted_url_count += 1

        for format_name in _available_format_ids(retrieved):
            if format_name in seen_formats:
                continue
            seen_formats.add(format_name)
            available_formats.append(format_name)

    completed_count = len(results)
    failed_count = len(validated["failed_items"])

    return BatchResultMetrics(
        requested_count=validated["requested_count"],
        completed_count=completed_count,
        failed_count=failed_count,
        retrieve_success_count=retrieve_success_count,
        hosted_url_count=hosted_url_count,
        size_exceeded_count=size_exceeded_count,
        partial_success=completed_count > 0 and failed_count > 0,
        batch_status=str(validated["batch"].get("status") or "").lower(),
        available_formats=available_formats,
    )


def build_completed_row_views(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    validated = validate_saved_output(payload)
    rows: List[Dict[str, Any]] = []

    for index, result in enumerate(validated["results"]):
        retrieved = result.get("retrieved") or {}
        if not isinstance(retrieved, Mapping):
            retrieved = {}

        available_format_ids = _available_format_ids(retrieved)
        hosted_format_ids = _hosted_format_ids(retrieved)
        row_id = str(result.get("retrieve_id") or result.get("custom_id") or index)
        url = str(result.get("url") or "")

        rows.append(
            {
                "row_id": row_id,
                "custom_id": str(result.get("custom_id") or ""),
                "url": url,
                "domain": _domain_for_url(url),
                "retrieve_id": str(result.get("retrieve_id") or ""),
                "success": bool(retrieved.get("success")),
                "size_exceeded": bool(retrieved.get("size_exceeded")),
                "available_formats": ", ".join(
                    format_name.title() for format_name in available_format_ids
                )
                or "None",
                "available_format_ids": available_format_ids,
                "hosted_formats": ", ".join(
                    format_name.title() for format_name in hosted_format_ids
                )
                or "None",
                "has_hosted_url": bool(hosted_format_ids),
                "preview": build_content_preview(retrieved),
                "result": result,
            }
        )

    return rows


def build_failed_row_views(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    validated = validate_saved_output(payload)
    rows: List[Dict[str, Any]] = []
    for index, item in enumerate(validated["failed_items"]):
        url = str(item.get("url") or "")
        rows.append(
            {
                "row_id": str(item.get("custom_id") or index),
                "custom_id": str(item.get("custom_id") or ""),
                "url": url,
                "domain": _domain_for_url(url),
            }
        )
    return rows


__all__ = [
    "ALLOWED_RETRIEVE_FORMATS",
    "BatchResultMetrics",
    "RetrieveFormat",
    "build_completed_row_views",
    "build_content_preview",
    "build_failed_row_views",
    "build_output_payload",
    "derive_result_metrics",
    "get_api_token",
    "load_saved_output",
    "parse_retrieve_formats",
    "poll_until_completed",
    "read_csv_items",
    "read_csv_items_from_bytes",
    "read_csv_items_from_text",
    "run_batch",
    "run_batch_sync",
    "save_output_payload",
    "validate_saved_output",
]
