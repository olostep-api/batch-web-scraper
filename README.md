# Batch Web Scraper (CSV → JSON)

Open-source batch web scraper built on the Olostep Batch API. Feed it a CSV of URLs and get back a single JSON file containing the extracted content (markdown/html/json), plus a list of any failed URLs.

## Features

- CSV input (`custom_id` or `id`, `url`) so outputs map back to your records
- Creates a batch, polls progress, then retrieves content (`markdown`/`html`/`json`)
- Handles large batches (thousands of URLs; Olostep supports up to ~10k per batch depending on your account)
- Cursor pagination (`cursor` + `limit`, recommended 10–50) when reading batch items
- Saves failed items too (`failed_count`, `failed_items`)
- Logs a warning when Olostep returns `size_exceeded=true` (content provided via hosted URLs)

## How it works

1) Read a CSV of URLs (with your own `custom_id` per row)
2) Create an Olostep batch
3) Poll until the batch completes
4) List completed/failed items (`GET /v1/batches/{batch_id}/items`)
5) Retrieve content for completed items (`GET /v1/retrieve`)
6) Write everything to a single JSON file

## Requirements

- Python 3.9+
- An Olostep API token/key

## Quick start

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export OLOSTEP_API_TOKEN="YOUR_TOKEN"
# or
export OLOSTEP_API_KEY="YOUR_TOKEN"
```

### Input CSV

Your CSV must have a header row and these columns:

* `custom_id` (or `id`) — your identifier so you can map results back to your records
* `url` — the page to process

Example: `data/urls_sample.csv`

```csv
custom_id,url
heat-003,https://heat.gov/tools-resources/cdc-heatrisk-dashboard/
heat-004,https://heat.gov/tools-resources/extreme-heat-vulnerability-mapping-tool/
```

### Run

```bash
python main.py \
  --csv data/urls_sample.csv \
  --out output.json \
  --country US \
  --formats markdown
```

You can also pass the token directly:

```bash
python main.py --csv data/urls_sample.csv --out output.json --token "YOUR_TOKEN"
```

## Common options

* `--country`: ISO 3166-1 alpha-2 country code (e.g. `US`, `IN`) (default: empty)
* `--parser-id`: use an Olostep Parser for structured extraction
* `--poll-seconds`: polling interval (default `5.0`)
* `--formats`: comma-separated list of retrieve formats (`markdown,html,json`)
* `--items-limit`: page size for `/v1/batches/{batch_id}/items` pagination (docs recommend `10–50`, default `50`)
* `--log-level`: log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) (default `INFO`, or `LOG_LEVEL` env var)

## Output JSON

The output file (e.g. `output.json`) contains:

* `batch` / `batch_id`: the final batch object (from `GET /v1/batches/{batch_id}`) and its ID
* `results`: one entry per completed item, including:

  * `custom_id`, `url`, `retrieve_id`
  * `retrieved`: the `/v1/retrieve` response (content fields depend on `--formats`)
* `failed_count` / `failed_items`: items returned by `GET /v1/batches/{batch_id}/items?status=failed`

Note: if content is too large, Olostep may return `*_hosted_url` fields instead of inline content. This repo logs a warning when `size_exceeded=true`. Hosted URLs expire after ~7 days, so store/download what you need soon.

### Example output (trimmed)

```json
{
  "batch": { "id": "batch_...", "status": "completed" },
  "batch_id": "batch_...",
  "requested_count": 2,
  "results_count": 2,
  "results": [
    {
      "custom_id": "heat-004",
      "url": "https://...",
      "retrieve_id": "...",
      "retrieved": {
        "success": true,
        "size_exceeded": false,
        "markdown_content": "...",
        "markdown_hosted_url": null
      }
    }
  ],
  "failed_count": 0,
  "failed_items": []
}
```

## Get an API key

* Olostep: [https://www.olostep.com/](https://www.olostep.com/)
