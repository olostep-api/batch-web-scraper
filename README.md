# Batch Web Scraper (CSV → JSON)

Open-source batch web scraper built on the Olostep Batch API. Feed it a CSV of URLs and get back a single JSON file containing the extracted content (markdown/html/json), plus a list of any failed URLs.

## Features

- CSV input (`custom_id`, `url`) so outputs map back to your records
- Creates a batch, polls progress, then retrieves content (`markdown`/`html`/`json`)
- Handles large batches (thousands of URLs; up to `~10k`) without you orchestrating per-URL scrapes
- Saves failed items too (`failed_count`, `failed_items`)

## How it works

1) Read a CSV of URLs (with your own `custom_id` per row)
2) Create an Olostep batch
3) Poll until the batch completes
4) Retrieve content for each completed item (`markdown` by default)
5) Write everything to a single JSON file

## Requirements

- Python 3.9+
- An Olostep API token

## Quick start

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export OLOSTEP_API_TOKEN="YOUR_TOKEN"
```

### Input CSV

Your CSV must have a header row and these columns:

- `custom_id` (or `id`) — your identifier so you can map results back to your records
- `url` — the page to process

Example: `data/urls.sample.csv`

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
  --country US \ ## Optional leave empty to make it random
  --formats markdown
```

You can also pass the token directly:

```bash
python main.py --csv data/urls.sample.csv --out output.json --token "YOUR_TOKEN"
```

### Common options

- `--country`: ISO 3166-1 alpha-2 country code (e.g. `US`, `IN`) (default: `RANDOM`)
- `--parser-id`: use an Olostep Parser for structured extraction
- `--poll-seconds`: polling interval (default `5.0`)
- `--formats`: comma-separated list of retrieve formats (`markdown,html,json`)

## Output JSON

The output file (e.g. `output.json`) contains:

- `batch` / `batch_id`: the final batch object (from `GET /v1/batches/{batch_id}`) and its ID
- `results`: one entry per completed item, including:
  - `custom_id`, `url`, `retrieve_id`
  - `retrieved`: the `/v1/retrieve` response (content fields depend on `--formats`)
- `failed_count` / `failed_items`: items returned by `GET /v1/batches/{batch_id}/items?status=failed` (useful when `results_count` is 0)

Note: if content is too large, Olostep may return `*_hosted_url` fields instead of inline content; this tool stores the response as-is.

### Example output (trimmed)

See `output_small.json` for a full example. Shape looks like:

> Note: the snippet below uses JSON-with-comments (`jsonc`) for explanation; the actual `output.json` is standard JSON (no comments).

```jsonc
{
  "batch": { // final batch object (GET /v1/batches/{batch_id})
    "id": "batch_...", // batch id (use to poll GET /v1/batches/{batch_id})
    "object": "batch", // API resource type label
    "status": "completed", // batch status at time of saving
    "created": 1769509515118, // Unix epoch milliseconds
    "total_urls": 2, // number of URLs submitted
    "completed_urls": 2, // number completed so far
    "number_retried": 0, // retry count
    "batch_parser": "none", // parser setting (batch)
    "parser": "none", // parser setting
    "batch_country": "RANDOM", // execution country (batch)
    "country": "RANDOM", // execution country
    "start_date": "2026-01-27", // YYYY-MM-DD
    "metadata": {} // your metadata (if any)
  },

  "batch_id": "batch_...", // convenience copy of batch.id
  "requested_count": 2, // number of valid rows read from the input CSV
  "results_count": 2, // number of entries written to `results`
  "results": [ // one entry per completed batch item (or per-item error)
    {
      "custom_id": "heat-004", // your identifier from CSV
      "url": "https://...", // URL processed
      "retrieve_id": "....", // ID used with GET /v1/retrieve
      "retrieved": { // retrieve response (GET /v1/retrieve)
        "success": true, // whether the retrieve request succeeded
        "size_exceeded": false, // if true, use hosted URLs instead of inline content
        "html_content": "<html>...</html>", // inline HTML (may be null)
        "markdown_content": "...", // inline markdown (may be null)
        "json_content": null, // structured JSON (may be null)
        "html_hosted_url": "https://...", // hosted HTML (may be null)
        "markdown_hosted_url": "https://...", // hosted markdown (may be null)
        "json_hosted_url": null, // hosted JSON (may be null)
        "network_calls": null // API telemetry/debug (may be null)
      }
    }
  ], // completed items
  "failed_count": 0, // number of failed items returned by the API
  "failed_items": [] // raw failed items (GET /v1/batches/{batch_id}/items?status=failed)
}
```

## Get an API key

- Olostep: `https://www.olostep.com/`
