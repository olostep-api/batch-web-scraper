from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, Union

import httpx


Formats = Sequence[Literal["html", "markdown", "json"]]


@dataclass(frozen=True)
class BatchProgress:
    is_completed: bool
    status: str
    total_urls: int
    completed_urls: int


class OlostepBatchClient:
    """
    Minimal async client for Olostep Batch API.

    Auth: Authorization: Bearer <token>  :contentReference[oaicite:0]{index=0}
    Endpoints used:
      - POST   /v1/batches                 :contentReference[oaicite:1]{index=1}
      - GET    /v1/batches/{batch_id}      :contentReference[oaicite:2]{index=2}
      - GET    /v1/batches/{batch_id}/items :contentReference[oaicite:3]{index=3}
      - GET    /v1/retrieve                :contentReference[oaicite:4]{index=4}
    """

    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api.olostep.com",
        timeout: float = 60.0,
    ) -> None:
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

    async def __aenter__(self) -> "OlostepBatchClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    # -----------------------------
    # Core: batches
    # -----------------------------
    async def create_batch(
        self,
        items: Union[
            Sequence[str],  # list of URLs
            Sequence[Dict[str, str]],  # [{"url": "...", "custom_id": "..."}]
        ],
        *,
        country: Optional[str] = None,
        parser_id: Optional[str] = None,
        links_on_page: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        webhook: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Creates a batch and returns the API response (includes batch "id").
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
        """
        Returns (is_completed, total_urls, completed_urls).
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
        """
        Returns the full batch JSON from GET /v1/batches/{batch_id}.
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
        """
        Returns one page of items. Response typically includes:
          - items: [...]
          - items_count
          - cursor (when more results exist)
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
        """
        Async generator over all items across paginated /items.
        """
        cursor: Optional[int] = None
        while True:
            page = await self.list_batch_items(batch_id, status=status, cursor=cursor)
            for item in page.get("items", []) or []:
                yield item

            # Olostep uses a numeric cursor when more pages exist.
            next_cursor = page.get("cursor", None)
            if next_cursor is None:
                break
            cursor = int(next_cursor)

    # -----------------------------
    # Core: retrieve
    # -----------------------------
    async def retrieve(
        self,
        retrieve_id: str,
        *,
        formats: Formats = ("markdown",),
    ) -> Dict[str, Any]:
        """
        Retrieves content for a single retrieve_id.
        If content is large, API may return hosted URLs + size_exceeded. :contentReference[oaicite:5]{index=5}
        """
        params: List[Tuple[str, str]] = [("retrieve_id", retrieve_id)]
        for f in formats:
            params.append(("formats[]", f))

        r = await self._client.get("/v1/retrieve", params=params)
        r.raise_for_status()
        return r.json()
