# parquet_utils.py
from __future__ import annotations
import io
import json
from typing import Any, Dict, Iterable, List, Tuple
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from datetime import datetime, timedelta, timezone

# ---------- Flatten helpers ----------
def _get(d: Dict, path: str, default=None):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def flatten_events(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for ev in doc.get("events", []) or []:
        rows.append({
            "doc_user": doc.get("user"),
            "doc_windowStartUtc": doc.get("windowStartUtc"),
            "doc_windowEndUtc": doc.get("windowEndUtc"),
            "doc_fetchedUtc": doc.get("fetchedUtc"),
            "event_id": ev.get("id"),
            "event_subject": ev.get("subject"),
            "event_start": _get(ev, "start.dateTime"),
            "event_start_tz": _get(ev, "start.timeZone"),
            "event_end": _get(ev, "end.dateTime"),
            "event_end_tz": _get(ev, "end.timeZone"),
            "event_isOnlineMeeting": ev.get("isOnlineMeeting"),
            "event_onlineMeetingUrl": ev.get("onlineMeetingUrl"),
            "event_onlineMeeting_joinUrl": _get(ev, "onlineMeeting.joinUrl"),
            "event_webLink": ev.get("webLink"),
            "event_location_displayName": _get(ev, "location.displayName"),
            "onlineMeetingMeta_id": _get(ev, "onlineMeetingMeta.id"),
        })
    return rows

def flatten_attendance_reports(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for ev in doc.get("events", []) or []:
        att = ev.get("attendance") or {}
        for rep in att.get("attendanceReports", []) or []:
            rows.append({
                "doc_user": doc.get("user"),
                "doc_fetchedUtc": doc.get("fetchedUtc"),
                "event_id": ev.get("id"),
                "event_subject": ev.get("subject"),
                "event_start": _get(ev, "start.dateTime"),
                "event_end": _get(ev, "end.dateTime"),
                "onlineMeetingId": att.get("onlineMeetingId"),
                "report_id": rep.get("report_id"),
                "meetingStartDateTime": rep.get("meetingStartDateTime"),
                "meetingEndDateTime": rep.get("meetingEndDateTime"),
                "total_participants": rep.get("total_participants"),
            })
    return rows

def flatten_attendance_records(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for ev in doc.get("events", []) or []:
        att = ev.get("attendance") or {}
        for rep in att.get("attendanceReports", []) or []:
            for rec in rep.get("records", []) or []:
                rows.append({
                    "doc_user": doc.get("user"),
                    "doc_fetchedUtc": doc.get("fetchedUtc"),
                    "event_id": ev.get("id"),
                    "event_subject": ev.get("subject"),
                    "event_start": _get(ev, "start.dateTime"),
                    "event_end": _get(ev, "end.dateTime"),
                    "onlineMeetingId": att.get("onlineMeetingId"),
                    "report_id": rep.get("report_id"),
                    "meetingStartDateTime": rep.get("meetingStartDateTime"),
                    "meetingEndDateTime": rep.get("meetingEndDateTime"),
                    "total_participants": rep.get("total_participants"),
                    "record_id": rec.get("id"),
                    "displayName": rec.get("displayName"),
                    "emailAddress": rec.get("emailAddress"),
                    "role": rec.get("role"),
                    "joinDateTime": rec.get("joinDateTime"),
                    "leaveDateTime": rec.get("leaveDateTime"),
                    "durationInSeconds": rec.get("durationInSeconds"),
                })
    return rows

# ---------- Public API: JSON -> DataFrames ----------
def json_docs_to_dataframes(docs: Iterable[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
    """
    Accepts an iterable of JSON documents (dicts) produced by your exporter.
    Returns dict of DataFrames: {'events': df, 'attendance_reports': df, 'attendance_records': df}
    """
    events_rows, reports_rows, records_rows = [], [], []
    for doc in docs:
        events_rows.extend(flatten_events(doc))
        reports_rows.extend(flatten_attendance_reports(doc))
        records_rows.extend(flatten_attendance_records(doc))

    df_events   = pd.DataFrame(events_rows or [{}]).dropna(how="all")
    df_reports  = pd.DataFrame(reports_rows or [{}]).dropna(how="all")
    df_records  = pd.DataFrame(records_rows or [{}]).dropna(how="all")

    # Normalize timestamps (optional but handy)
    for col in [
        "doc_fetchedUtc", "event_start", "event_end",
        "meetingStartDateTime", "meetingEndDateTime",
        "joinDateTime", "leaveDateTime"
    ]:
        for df in (df_events, df_reports, df_records):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return {
        "events": df_events,
        "attendance_reports": df_reports,
        "attendance_details_records": df_records
    }
def write_parquet_blob(
    dfs: Dict[str, pd.DataFrame],
    account_url: str,
    container: str,
    overwrite: bool = True,
    app_prefix = "msteams",

) -> Dict[str, str]:
    """
    Writes Parquet files to Azure Blob at <container>/<prefix>/<name>.parquet.
    Returns dict of blob URLs.
    """
    now = datetime.now(timezone.utc)
    cred = DefaultAzureCredential()
    svc  = BlobServiceClient(account_url=account_url, credential=cred)
    cc   = svc.get_container_client(container)
    try:
        cc.create_container()
    except Exception:
        pass

    outputs = {}
    for name, df in dfs.items():
        if df.empty:
            continue
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        blob_name = f"{app_prefix}/parquet/{now:%Y/%m/%d}/{name}.parquet"
        bc = cc.get_blob_client(blob_name)
        bc.upload_blob(
            buf.getvalue(),
            overwrite=overwrite,
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )
        outputs[name] = f"{account_url.rstrip('/')}/{container}/{blob_name}"
    return outputs
