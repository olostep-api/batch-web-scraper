from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union

import httpx


Formats = Sequence[Literal["html", "markdown", "json"]]


@dataclass(frozen=True)
class BatchProgress:
    is_completed: bool
    status: str
    total_urls: int
    completed_urls: int


class BatchScraper:
    """:
    Async client for the Olostep Batch API.

    Endpoints used:
        - POST /v1/batches
        - GET /v1/batches/{batch_id}
        - GET /v1/batches/{batch_id}/items
        - GET /v1/retrieve
    """

    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api.olostep.com",
        timeout: float = 60.0,
    ) -> None:
        """:
            Create a `BatchScraper`.

        Args:
            api_token: Olostep API token.
            base_url: Olostep API base URL.
            timeout: Per-request timeout in seconds.

        Returns:
            None
        """
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {api_token}",
                "Accept": "application/json",
            },
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "BatchScraper":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def create_batch(
        self,
        items: Union[
            Sequence[str],
            Sequence[Dict[str, str]],
        ],
        *,
        country: Optional[str] = None,
        parser_id: Optional[str] = None,
        links_on_page: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        webhook: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """:
            Create a batch.

        Args:
            items: Either a list of URLs or a list of dicts with at least `url` (optional `custom_id`).
            country: Optional country code (e.g. `US`).
            parser_id: Optional parser id for structured extraction.
            links_on_page: Optional Olostep `links_on_page` configuration.
            metadata: Optional metadata passed through to the API.
            webhook: Optional webhook configuration.

        Returns:
            The batch create response JSON (includes `id`).
        """
        normalized_items: List[Dict[str, str]] = []
        if items and isinstance(items[0], str):
            for i, url in enumerate(items):  # type: ignore[arg-type]
                normalized_items.append({"custom_id": str(i), "url": url})
        else:
            for it in items:  # type: ignore[assignment]
                if "url" not in it:
                    raise ValueError("Each item dict must contain 'url'.")
                normalized_items.append(
                    {"url": it["url"], "custom_id": it.get("custom_id", it["url"])}
                )

        payload: Dict[str, Any] = {"items": normalized_items}
        if country:
            payload["country"] = country
        if parser_id:
            payload["parser"] = {"id": parser_id}
        if links_on_page:
            payload["links_on_page"] = links_on_page
        if metadata:
            payload["metadata"] = metadata
        if webhook:
            payload["webhook"] = webhook

        r = await self._client.post("/v1/batches", json=payload)
        r.raise_for_status()
        return r.json()

    async def get_batch_progress(self, batch_id: str) -> BatchProgress:
        """:
            Fetch progress information for a batch.

        Args:
            batch_id: Batch id returned by `create_batch`.

        Returns:
            A `BatchProgress` with status and completed/total counts.
        """
        r = await self._client.get(f"/v1/batches/{batch_id}")
        r.raise_for_status()
        data = r.json()

        status = str(data.get("status", "")).lower()
        total = int(data.get("total_urls") or 0)
        completed = int(data.get("completed_urls") or 0)
        return BatchProgress(
            is_completed=(status == "completed"),
            status=status,
            total_urls=total,
            completed_urls=completed,
        )

    async def get_batch(self, batch_id: str) -> Dict[str, Any]:
        """:
            Fetch the full batch object.

        Args:
            batch_id: Batch id returned by `create_batch`.

        Returns:
            The batch JSON returned by the API.
        """
        r = await self._client.get(f"/v1/batches/{batch_id}")
        r.raise_for_status()
        return r.json()

    async def list_batch_items(
        self,
        batch_id: str,
        *,
        status: Optional[Literal["completed", "failed", "in_progress"]] = None,
        cursor: Optional[int] = None,
    ) -> Dict[str, Any]:
        """:
            Fetch one page of items for a batch.

        Args:
            batch_id: Batch id returned by `create_batch`.
            status: Optional filter for item status.
            cursor: Optional pagination cursor.

        Returns:
            A page response JSON (typically includes `items` and optionally `cursor`).
        """
        params: Dict[str, Any] = {}
        if status:
            params["status"] = status
        if cursor is not None:
            params["cursor"] = cursor

        r = await self._client.get(f"/v1/batches/{batch_id}/items", params=params)
        r.raise_for_status()
        return r.json()

    async def iter_batch_items(
        self,
        batch_id: str,
        *,
        status: Optional[Literal["completed", "failed", "in_progress"]] = None,
    ):
        """:
            Iterate all items for a batch across pagination.

        Args:
            batch_id: Batch id returned by `create_batch`.
            status: Optional filter for item status.

        Returns:
            An async iterator yielding item dicts from the API.
        """
        cursor: Optional[int] = None
        while True:
            page = await self.list_batch_items(batch_id, status=status, cursor=cursor)
            for item in page.get("items", []) or []:
                yield item

            next_cursor = page.get("cursor", None)
            if next_cursor is None:
                break
            cursor = int(next_cursor)

    async def retrieve(
        self,
        retrieve_id: str,
        *,
        formats: Formats = ("markdown",),
    ) -> Dict[str, Any]:
        """:
            Retrieve content for a single `retrieve_id`.

        Args:
            retrieve_id: Retrieve id from a completed batch item.
            formats: Content formats to request (e.g. `("markdown", "html")`).

        Returns:
            The retrieve response JSON (content fields depend on `formats`).
        """
        params: List[Tuple[str, str]] = [("retrieve_id", retrieve_id)]
        for f in formats:
            params.append(("formats[]", f))

        r = await self._client.get("/v1/retrieve", params=params)
        r.raise_for_status()
        return r.json()


OlostepBatchClient = BatchScraper
