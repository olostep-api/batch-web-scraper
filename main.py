import argparse
import sys

from loguru import logger

from src.batch_workflow import (
    get_api_token,
    parse_retrieve_formats,
    read_csv_items,
    run_batch_sync,
    save_output_payload,
)


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

    token = get_api_token(args.token)
    if not token:
        raise SystemExit(
            "Missing API token. Pass --token or set OLOSTEP_API_TOKEN / OLOSTEP_API_KEY."
        )

    try:
        retrieve_formats = parse_retrieve_formats(args.formats)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    if args.items_limit < 1:
        raise SystemExit("--items-limit must be >= 1")

    items = read_csv_items(args.csv)
    payload = run_batch_sync(
        items,
        token,
        country=args.country,
        parser_id=args.parser_id,
        poll_seconds=args.poll_seconds,
        retrieve_formats=retrieve_formats,
        log_every_n_polls=args.log_every,
        items_limit=args.items_limit,
    )
    save_output_payload(payload, args.out)
    logger.info(f"Saved: {args.out} (results={payload['results_count']})")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.error("Interrupted by user.")
        sys.exit(130)
