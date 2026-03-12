from __future__ import annotations

import html
import json
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import streamlit as st

from src.batch_workflow import (
    ALLOWED_RETRIEVE_FORMATS,
    derive_result_metrics,
    get_api_token,
    read_csv_items_from_bytes,
    run_batch_sync,
)

st.set_page_config(
    page_title="Olostep Batch Workspace",
    page_icon="O",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@500;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap');

        :root {
            --primary: #635bff;
            --primary-hover: #534ae6;
            --bg-base: #f9fafb;
            --surface: #ffffff;
            --surface-soft: #f4f6ff;
            --text-main: #13152b;
            --text-muted: #5c6370;
            --border: #dce0f0;
            --accent-1: #eceffd;
            --accent-2: #e2e6ff;
            --success: #0f8f5f;
            --error: #c2362b;
        }

        html, body, [class*="css"] {
            font-family: 'Manrope', 'Avenir Next', 'Segoe UI', sans-serif;
        }

        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top right, rgba(99, 91, 255, 0.12), transparent 38%),
                linear-gradient(180deg, rgba(241, 243, 255, 0.85) 0px, rgba(249, 250, 251, 0.98) 220px),
                var(--bg-base);
            color: var(--text-main);
        }

        [data-testid="stMainBlockContainer"] {
            max-width: 1040px;
            padding-top: 1.25rem;
            padding-bottom: 2.5rem;
        }

        .hero-bubble {
            background: linear-gradient(135deg, rgba(99, 91, 255, 0.94) 0%, rgba(181, 177, 255, 0.78) 42%, rgba(236, 239, 253, 0.98) 78%, rgba(255, 255, 255, 0.98) 100%);
            border: 1px solid rgba(99, 91, 255, 0.16);
            border-radius: 22px;
            padding: 1.25rem 1.35rem;
            box-shadow: 0 1px 0 rgba(19, 21, 43, 0.02);
        }

        .eyebrow {
            display: inline-block;
            font-size: 0.72rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #3127af;
            font-weight: 800;
            margin-bottom: 0.5rem;
        }

        .page-title {
            margin: 0;
            font-size: 1.7rem;
            line-height: 1.08;
            color: var(--text-main);
            font-weight: 800;
        }

        .page-subtitle {
            margin: 0.45rem 0 0;
            color: rgba(19, 21, 43, 0.82);
            font-size: 0.95rem;
            line-height: 1.45;
            max-width: 54ch;
        }

        .section-head {
            margin-bottom: 0.85rem;
        }

        .section-title {
            margin: 0;
            font-size: 1rem;
            font-weight: 800;
            color: var(--text-main);
        }

        .section-subtitle {
            margin: 0.24rem 0 0;
            color: var(--text-muted);
            font-size: 0.84rem;
            line-height: 1.45;
        }

        .small-note {
            color: var(--text-muted);
            font-size: 0.82rem;
            line-height: 1.45;
        }

        .log-box {
            background: #050608;
            border: 1px solid rgba(5, 6, 8, 0.82);
            border-radius: 14px;
            min-height: 320px;
            max-height: 420px;
            overflow: auto;
            padding: 0.9rem 1rem;
        }

        .log-box pre {
            margin: 0;
            color: #eef1ff;
            font-size: 0.82rem;
            line-height: 1.55;
            white-space: pre-wrap;
            word-break: break-word;
            font-family: 'IBM Plex Mono', ui-monospace, monospace;
        }

        .status-card {
            background: var(--surface-soft);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 0.95rem 1rem;
        }

        .status-top {
            display: flex;
            justify-content: space-between;
            gap: 0.9rem;
            align-items: flex-start;
            flex-wrap: wrap;
        }

        .status-title {
            margin: 0;
            font-size: 1rem;
            font-weight: 800;
            color: var(--text-main);
        }

        .status-copy {
            margin: 0.3rem 0 0;
            color: var(--text-muted);
            font-size: 0.85rem;
            line-height: 1.45;
        }

        .status-meta {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.75rem;
            margin-top: 0.85rem;
        }

        .status-key {
            color: var(--text-muted);
            font-size: 0.74rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 800;
            margin-bottom: 0.18rem;
        }

        .status-value {
            color: var(--text-main);
            font-size: 0.92rem;
            line-height: 1.25;
            font-weight: 800;
            word-break: break-word;
        }

        .pill {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.26rem 0.62rem;
            font-size: 0.76rem;
            font-weight: 800;
            border: 1px solid transparent;
        }

        .pill-neutral {
            background: rgba(19, 21, 43, 0.05);
            color: var(--text-main);
            border-color: var(--border);
        }

        .pill-primary {
            background: rgba(99, 91, 255, 0.12);
            color: var(--primary);
            border-color: rgba(99, 91, 255, 0.16);
        }

        .pill-success {
            background: rgba(15, 143, 95, 0.1);
            color: var(--success);
            border-color: rgba(15, 143, 95, 0.16);
        }

        .pill-error {
            background: rgba(194, 54, 43, 0.1);
            color: var(--error);
            border-color: rgba(194, 54, 43, 0.16);
        }

        .summary-strip {
            background: var(--surface-soft);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 0.95rem 1rem;
        }

        .summary-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.75rem;
        }

        .summary-label {
            color: var(--text-muted);
            font-size: 0.74rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 800;
            margin-bottom: 0.22rem;
        }

        .summary-value {
            color: var(--text-main);
            font-size: 1.15rem;
            line-height: 1.1;
            font-weight: 800;
        }

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1rem;
            background: rgba(255, 255, 255, 0.92);
            box-shadow: 0 1px 0 rgba(19, 21, 43, 0.02);
        }

        .stButton > button,
        .stDownloadButton > button {
            border-radius: 10px;
            font-weight: 800;
            min-height: 2.6rem;
            border: 1px solid var(--border);
            background: var(--surface);
            color: var(--text-main);
            padding: 0.55rem 0.9rem;
        }

        .stButton > button *,
        .stDownloadButton > button * {
            color: var(--text-main) !important;
            fill: var(--text-main) !important;
        }

        .stButton > button[kind="primary"],
        .stDownloadButton > button[kind="primary"] {
            background: var(--primary);
            color: #ffffff;
            border-color: var(--primary);
        }

        .stButton > button[kind="primary"] *,
        .stDownloadButton > button[kind="primary"] * {
            color: #ffffff !important;
            fill: #ffffff !important;
        }

        .stButton > button:hover,
        .stDownloadButton > button:hover {
            border-color: var(--primary);
            color: var(--primary);
        }

        .stButton > button:hover *,
        .stDownloadButton > button:hover * {
            color: var(--primary) !important;
            fill: var(--primary) !important;
        }

        .stButton > button[kind="primary"]:hover,
        .stDownloadButton > button[kind="primary"]:hover {
            background: var(--primary-hover);
            color: #ffffff;
            border-color: var(--primary-hover);
        }

        .stButton > button[kind="primary"]:hover *,
        .stDownloadButton > button[kind="primary"]:hover * {
            color: #ffffff !important;
            fill: #ffffff !important;
        }

        .stFileUploader button,
        .stFileUploader button * {
            color: var(--text-main) !important;
            fill: var(--text-main) !important;
        }

        .stMultiSelect div[data-baseweb="select"] > div,
        .stFileUploader,
        .stProgress > div > div {
            border-radius: 12px;
        }

        .stMultiSelect div[data-baseweb="select"] > div {
            border: 1px solid var(--border);
            background: rgba(255, 255, 255, 0.95);
            color: var(--text-main);
        }

        .stMultiSelect label,
        .stFileUploader label {
            font-weight: 800;
            color: var(--text-main);
        }

        .stProgress > div > div {
            background: var(--accent-1);
        }

        .stProgress > div > div > div {
            background: var(--primary);
        }

        .stAlert {
            border-radius: 12px;
            border: 1px solid var(--border);
        }

        .stTabs [data-baseweb="tab-list"] {
            gap: 0.35rem;
            margin-bottom: 0.85rem;
        }

        .stTabs [data-baseweb="tab"] {
            height: auto;
            padding: 0.5rem 0.9rem;
            border-radius: 999px;
            background: rgba(241, 243, 255, 0.88);
            color: var(--text-muted);
            font-weight: 800;
            border: 1px solid transparent;
        }

        .stTabs [data-baseweb="tab"] * {
            color: var(--text-muted) !important;
        }

        .stTabs [aria-selected="true"] {
            background: var(--surface);
            color: var(--primary);
            border-color: rgba(99, 91, 255, 0.14);
        }

        .stTabs [aria-selected="true"] * {
            color: var(--primary) !important;
        }

        @media (max-width: 900px) {
            .summary-grid {
                grid-template-columns: 1fr 1fr;
            }

            .status-meta {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def ensure_state() -> None:
    defaults = {
        "run_logs": [],
        "run_payload": None,
        "run_payload_json": None,
        "run_status": {
            "phase": "idle",
            "message": "Select a CSV file, choose the formats, and start the batch run.",
            "batch_id": None,
            "elapsed_seconds": 0,
        },
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def render_header() -> None:
    st.markdown(
        """
        <div class="hero-bubble">
            <span class="eyebrow">Olostep Batch Scraper</span>
            <h1 class="page-title">Simple batch run</h1>
            <p class="page-subtitle">
                Select a CSV, choose the formats you want back, run the batch,
                and watch the logs stream in.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_head(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="section-head">
            <p class="section-title">{esc(title)}</p>
            <p class="section-subtitle">{esc(subtitle)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def read_uploaded_items(uploaded_file: Any) -> Tuple[Optional[List[Dict[str, str]]], Optional[str]]:
    if uploaded_file is None:
        return None, None
    try:
        return read_csv_items_from_bytes(uploaded_file.getvalue()), None
    except ValueError as exc:
        return None, str(exc)


def append_log(message: str) -> None:
    logs = st.session_state["run_logs"]
    line = f"[{time.strftime('%H:%M:%S')}] {message}"
    if logs and logs[-1] == line:
        return
    logs.append(line)
    st.session_state["run_logs"] = logs[-250:]


def status_pill(label: str, tone: str) -> str:
    return f"<span class='pill pill-{esc(tone)}'>{esc(label)}</span>"


def build_status_view(state: Dict[str, Any]) -> Tuple[str, float, str]:
    phase = str(state.get("phase") or "idle")
    message = str(state.get("message") or "Waiting to start.")
    batch_id = state.get("batch_id") or "Not created yet"
    elapsed = state.get("elapsed_seconds") or 0
    completed_urls = int(state.get("completed_urls") or 0)
    total_urls = int(state.get("total_urls") or 0)
    processed_results = int(state.get("processed_results") or 0)
    expected_results = int(state.get("expected_results") or 0)

    if phase == "idle":
        title = "Ready"
        tone = "neutral"
        progress_value = 0.0
        progress_label = "Waiting to start"
    elif phase == "starting":
        title = "Starting"
        tone = "primary"
        progress_value = 0.02
        progress_label = "Creating batch"
    elif phase == "batch_created":
        title = "Batch created"
        tone = "primary"
        progress_value = 0.08
        progress_label = "Batch queued"
    elif phase == "polling":
        title = "Running"
        tone = "primary"
        progress_value = (completed_urls / total_urls) if total_urls else 0.12
        progress_label = f"Batch progress {completed_urls}/{total_urls}"
    elif phase == "retrieving":
        title = "Retrieving"
        tone = "primary"
        progress_value = (
            processed_results / expected_results if expected_results else 0.92
        )
        progress_label = f"Retrieving {processed_results}/{expected_results or processed_results}"
    elif phase == "finalizing":
        title = "Finalizing"
        tone = "primary"
        progress_value = 0.98
        progress_label = "Preparing output"
    elif phase == "completed":
        title = "Completed"
        tone = "success"
        progress_value = 1.0
        progress_label = "Batch complete"
    else:
        title = "Error"
        tone = "error"
        progress_value = 1.0
        progress_label = "Run failed"

    status_html = f"""
        <div class='status-card'>
            <div class='status-top'>
                <div>
                    <p class='status-title'>{esc(title)}</p>
                    <p class='status-copy'>{esc(message)}</p>
                </div>
                <div>{status_pill(progress_label, tone)}</div>
            </div>
            <div class='status-meta'>
                <div>
                    <div class='status-key'>Batch ID</div>
                    <div class='status-value'>{esc(batch_id)}</div>
                </div>
                <div>
                    <div class='status-key'>Phase</div>
                    <div class='status-value'>{esc(phase.replace('_', ' ').title())}</div>
                </div>
                <div>
                    <div class='status-key'>Elapsed</div>
                    <div class='status-value'>{esc(f'{elapsed}s')}</div>
                </div>
            </div>
        </div>
    """
    return status_html, max(0.0, min(progress_value, 1.0)), progress_label


def render_status_box(
    status_placeholder: st.delta_generator.DeltaGenerator,
    progress_placeholder: st.delta_generator.DeltaGenerator,
    note_placeholder: st.delta_generator.DeltaGenerator,
) -> None:
    state = st.session_state.get("run_status") or {}
    status_html, progress_value, progress_label = build_status_view(state)
    status_placeholder.markdown(status_html, unsafe_allow_html=True)
    progress_placeholder.progress(progress_value, text=progress_label)

    phase = state.get("phase")
    message = str(state.get("message") or "")
    if phase == "completed":
        note_placeholder.success(message)
    elif phase == "error":
        note_placeholder.error(message)
    elif phase in {"starting", "batch_created", "polling", "retrieving", "finalizing"}:
        note_placeholder.info(message)
    else:
        note_placeholder.empty()


def render_log_box(placeholder: st.delta_generator.DeltaGenerator) -> None:
    logs = st.session_state.get("run_logs") or []
    content = "\n".join(logs) if logs else "Logs will appear here when you start a batch run."
    placeholder.markdown(
        f"<div class='log-box'><pre>{esc(content)}</pre></div>",
        unsafe_allow_html=True,
    )


def run_batch_job(
    items: Sequence[Dict[str, str]],
    formats: Sequence[str],
    status_placeholder: st.delta_generator.DeltaGenerator,
    progress_placeholder: st.delta_generator.DeltaGenerator,
    note_placeholder: st.delta_generator.DeltaGenerator,
    log_placeholder: st.delta_generator.DeltaGenerator,
) -> Optional[Dict[str, Any]]:
    token = get_api_token(None)
    if not token:
        append_log("Missing API token. Configure OLOSTEP_API_TOKEN or OLOSTEP_API_KEY in the environment or .env.")
        st.session_state["run_status"] = {
            "phase": "error",
            "message": "Missing API token. Configure OLOSTEP_API_TOKEN or OLOSTEP_API_KEY in the environment or .env.",
            "batch_id": None,
            "elapsed_seconds": 0,
        }
        render_status_box(status_placeholder, progress_placeholder, note_placeholder)
        render_log_box(log_placeholder)
        return None

    start_time = time.time()
    last_message = {"value": None}
    st.session_state["run_status"] = {
        "phase": "starting",
        "message": "Submitting batch to Olostep.",
        "batch_id": None,
        "elapsed_seconds": 0,
    }
    render_status_box(status_placeholder, progress_placeholder, note_placeholder)

    def on_progress(event: Dict[str, Any]) -> None:
        current_state = dict(st.session_state.get("run_status") or {})
        current_state.update(event)
        current_state["elapsed_seconds"] = int(time.time() - start_time)
        st.session_state["run_status"] = current_state
        render_status_box(status_placeholder, progress_placeholder, note_placeholder)

        message = str(event.get("message") or event.get("phase") or "Running")
        if message != last_message["value"]:
            append_log(message)
            last_message["value"] = message
            render_log_box(log_placeholder)

    try:
        append_log("Starting batch run.")
        render_log_box(log_placeholder)
        payload = run_batch_sync(
            items,
            token,
            retrieve_formats=list(formats),
            progress_callback=on_progress,
        )
    except Exception as exc:
        st.session_state["run_status"] = {
            "phase": "error",
            "message": str(exc),
            "batch_id": (st.session_state.get("run_status") or {}).get("batch_id"),
            "elapsed_seconds": int(time.time() - start_time),
        }
        render_status_box(status_placeholder, progress_placeholder, note_placeholder)
        append_log(f"Run failed: {exc}")
        render_log_box(log_placeholder)
        return None

    elapsed = int(time.time() - start_time)
    current_state = dict(st.session_state.get("run_status") or {})
    current_state.update(
        {
            "phase": "completed",
            "message": (
                f"Finished in {elapsed}s. Completed {payload['results_count']} item(s) with "
                f"{payload['failed_count']} failure(s)."
            ),
            "batch_id": payload.get("batch_id"),
            "elapsed_seconds": elapsed,
        }
    )
    st.session_state["run_status"] = current_state
    render_status_box(status_placeholder, progress_placeholder, note_placeholder)
    append_log(
        f"Finished in {elapsed}s. Completed {payload['results_count']} item(s) with {payload['failed_count']} failure(s)."
    )
    render_log_box(log_placeholder)
    return payload


def render_result_summary(payload: Dict[str, Any]) -> None:
    metrics = derive_result_metrics(payload)
    st.markdown(
        f"""
        <div class="summary-strip">
            <div class="summary-grid">
                <div>
                    <div class="summary-label">Batch ID</div>
                    <div class="summary-value">{esc(payload.get('batch_id') or '-')}</div>
                </div>
                <div>
                    <div class="summary-label">Requested</div>
                    <div class="summary-value">{metrics.requested_count}</div>
                </div>
                <div>
                    <div class="summary-label">Completed</div>
                    <div class="summary-value">{metrics.completed_count}</div>
                </div>
                <div>
                    <div class="summary-label">Failed</div>
                    <div class="summary-value">{metrics.failed_count}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.download_button(
        "Download JSON",
        data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        file_name="batch_output.json",
        mime="application/json",
        type="primary",
        use_container_width=True,
    )


def main() -> None:
    inject_styles()
    ensure_state()
    render_header()
    st.markdown("<div style='height:0.9rem;'></div>", unsafe_allow_html=True)

    uploaded_file = None
    items: Optional[List[Dict[str, str]]] = None
    source_error: Optional[str] = None
    selected_formats: Sequence[str] = ["markdown"]
    run_clicked = False
    token_available = bool(get_api_token(None))

    uploader_col, format_col = st.columns([1.25, 1.0], gap="large")
    with uploader_col:
        with st.container(border=True):
            render_section_head(
                "1. Select file",
                "Upload a CSV with custom_id or id, plus url.",
            )
            uploaded_file = st.file_uploader(
                "CSV file",
                type=["csv"],
                label_visibility="collapsed",
                key="csv_uploader",
            )
            items, source_error = read_uploaded_items(uploaded_file)
            if uploaded_file is None:
                st.markdown(
                    "<p class='small-note'>No file selected yet.</p>",
                    unsafe_allow_html=True,
                )
            elif source_error:
                st.error(source_error)
            else:
                st.markdown(
                    f"<p class='small-note'><strong>{esc(uploaded_file.name)}</strong> loaded with <strong>{len(items or [])}</strong> valid row(s).</p>",
                    unsafe_allow_html=True,
                )

    with format_col:
        with st.container(border=True):
            render_section_head(
                "2. Select formats",
                "Choose which retrieve payloads should be requested for each completed item.",
            )
            selected_formats = st.multiselect(
                "Formats",
                options=list(ALLOWED_RETRIEVE_FORMATS),
                default=["markdown"],
                label_visibility="collapsed",
                key="format_selector",
            )
            st.markdown(
                f"<p class='small-note'>Token status: <strong>{'configured' if token_available else 'missing'}</strong>.</p>",
                unsafe_allow_html=True,
            )
            run_disabled = uploaded_file is None or bool(source_error) or not token_available
            run_clicked = st.button(
                "Run batch scrape",
                type="primary",
                use_container_width=True,
                disabled=run_disabled,
            )

    with st.container(border=True):
        render_section_head(
            "3. Progress",
            "Default to live status for simple monitoring, or switch to logs for detailed progress lines.",
        )
        status_tab, logs_tab = st.tabs(["Live status", "Logs"])
        with status_tab:
            status_placeholder = st.empty()
            progress_placeholder = st.empty()
            note_placeholder = st.empty()
            render_status_box(status_placeholder, progress_placeholder, note_placeholder)
        with logs_tab:
            log_placeholder = st.empty()
            render_log_box(log_placeholder)

    if run_clicked:
        st.session_state["run_logs"] = []
        st.session_state["run_payload"] = None
        st.session_state["run_payload_json"] = None
        st.session_state["run_status"] = {
            "phase": "starting",
            "message": "Preparing the batch run.",
            "batch_id": None,
            "elapsed_seconds": 0,
        }
        render_status_box(status_placeholder, progress_placeholder, note_placeholder)
        render_log_box(log_placeholder)

        if uploaded_file is None:
            append_log("Please select a CSV file.")
            st.session_state["run_status"] = {
                "phase": "error",
                "message": "Please select a CSV file before starting the batch.",
                "batch_id": None,
                "elapsed_seconds": 0,
            }
            render_status_box(status_placeholder, progress_placeholder, note_placeholder)
            render_log_box(log_placeholder)
        elif source_error:
            append_log(source_error)
            st.session_state["run_status"] = {
                "phase": "error",
                "message": source_error,
                "batch_id": None,
                "elapsed_seconds": 0,
            }
            render_status_box(status_placeholder, progress_placeholder, note_placeholder)
            render_log_box(log_placeholder)
        elif not token_available:
            append_log("Missing API token. Configure OLOSTEP_API_TOKEN or OLOSTEP_API_KEY in the environment or .env.")
            st.session_state["run_status"] = {
                "phase": "error",
                "message": "Missing API token. Configure OLOSTEP_API_TOKEN or OLOSTEP_API_KEY in the environment or .env.",
                "batch_id": None,
                "elapsed_seconds": 0,
            }
            render_status_box(status_placeholder, progress_placeholder, note_placeholder)
            render_log_box(log_placeholder)
        elif items is not None:
            payload = run_batch_job(
                items,
                selected_formats or ["markdown"],
                status_placeholder,
                progress_placeholder,
                note_placeholder,
                log_placeholder,
            )
            if payload is not None:
                st.session_state["run_payload"] = payload
                st.session_state["run_payload_json"] = json.dumps(
                    payload, ensure_ascii=False, indent=2
                )

    latest_payload = st.session_state.get("run_payload")
    if latest_payload:
        st.markdown("<div style='height:0.85rem;'></div>", unsafe_allow_html=True)
        with st.container(border=True):
            render_section_head(
                "Run complete",
                "A compact summary plus export action.",
            )
            render_result_summary(latest_payload)


if __name__ == "__main__":
    main()
