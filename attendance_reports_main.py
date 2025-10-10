# pip install msal requests
import os, msal, requests
import sys
import json
from datetime import datetime, timedelta, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import config as cfg

TENANT        = cfg.get_details("tenant")
CLIENT_ID     = cfg.get_details("client_id")
CLIENT_SECRET = cfg.get_details("client_scret")
SGA_UPN       = cfg.get_details("sga_upn")

# ===== Registry-in-Blob settings (override via env if you like) =====
REG_ACCOUNT_URL  = os.getenv("REG_ACCOUNT_URL",  "https://sgaanalyticsstorageacnt.blob.core.windows.net")
REG_CONTAINER    = os.getenv("REG_CONTAINER",    "staging")
REG_BLOB_NAME    = os.getenv("REG_BLOB_NAME",    "msteams/registry/latest_meeting_start.txt")

# ---------- Blob helpers ----------
def _blob_client(account_url: str, container: str, blob_name: str):
    cred = DefaultAzureCredential()
    svc  = BlobServiceClient(account_url=account_url, credential=cred)
    # Ensure container exists
    try:
        svc.get_container_client(container).create_container()
    except ResourceExistsError:
        pass
    return svc.get_blob_client(container=container, blob=blob_name)

def save_json_to_blob(
    json_payload,
    account_url="https://sgaanalyticsstorageacnt.blob.core.windows.net",
    container="staging",
    app_prefix="msteams",
    blob_name=None,
    file_name = "",
    overwrite=False
) -> str:
    cred = DefaultAzureCredential()
    svc  = BlobServiceClient(account_url=account_url, credential=cred)
    try:
        svc.get_container_client(container).create_container()
    except ResourceExistsError:
        pass

    if not blob_name:
        now = datetime.now(timezone.utc)
        ts  = now.strftime("%Y%m%dT%H%M%SZ")
        blob_name = f"{app_prefix}/{now:%Y/%m/%d}/{file_name}.json"

    if isinstance(json_payload, (dict, list)):
        data = json.dumps(json_payload, ensure_ascii=False, indent=2).encode("utf-8")
    elif isinstance(json_payload, str):
        data = json_payload.encode("utf-8")
    elif isinstance(json_payload, bytes):
        data = json_payload
    else:
        raise TypeError("json_payload must be dict, list, str, or bytes")

    blob = svc.get_blob_client(container, blob_name)
    blob.upload_blob(
        data=data, overwrite=overwrite,
        content_settings=ContentSettings(content_type="application/json"),
    )
    return f"{account_url.rstrip('/')}/{container}/{blob_name}"

# ---------- Registry (checkpoint) in Blob ----------
def to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def parse_graph_datetime(dt_str: str) -> datetime | None:
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def load_checkpoint_from_blob() -> datetime | None:
    """
    Reads the latest meeting start (ISO8601 Z) from a small text blob.
    Returns None if it doesn't exist or is invalid.
    """
    bc = _blob_client(REG_ACCOUNT_URL, REG_CONTAINER, REG_BLOB_NAME)
    try:
        data = bc.download_blob().readall().decode("utf-8").strip()
        if not data:
            return None
        s = data.replace("Z", "+00:00") if data.endswith("Z") else data
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ResourceNotFoundError:
        return None
    except Exception:
        return None

def save_checkpoint_to_blob(latest_dt: datetime) -> str:
    """
    Writes the latest meeting start (ISO8601 Z) to the text blob.
    Overwrites existing.
    """
    bc = _blob_client(REG_ACCOUNT_URL, REG_CONTAINER, REG_BLOB_NAME)
    content = to_iso_z(latest_dt).encode("utf-8")
    bc.upload_blob(
        data=content,
        overwrite=True,
        content_settings=ContentSettings(content_type="text/plain; charset=utf-8"),
    )
    # Return URL for logging
    return f"{REG_ACCOUNT_URL.rstrip('/')}/{REG_CONTAINER}/{REG_BLOB_NAME}"

# ---------- Auth ----------
def acquire_app_token(tenant: str, client_id: str, client_secret: str) -> str:
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant}",
        client_credential=client_secret
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result.get('error_description')}")
    return result["access_token"]

# ---------- Graph paging ----------
def graph_paged_get(start_url: str, headers: dict, params: dict | None = None):
    url = start_url
    first = True
    while url:
        if first and params:
            r = requests.get(url, headers=headers, params=params, timeout=60)
            first = False
        else:
            r = requests.get(url, headers=headers, timeout=60)
        try:
            r.raise_for_status()
        except Exception:
            raise RuntimeError(f"Graph GET failed {r.status_code}: {r.text}")
        payload = r.json()
        yield payload
        url = payload.get("@odata.nextLink")

# ---------- Fetch events ----------
def fetch_all_events(headers: dict, user_upn: str, use_calendar_view=False,
                     start_dt_iso=None, end_dt_iso=None) -> list:
    if use_calendar_view:
        if not (start_dt_iso and end_dt_iso):
            raise ValueError("start_dt_iso and end_dt_iso are required when use_calendar_view=True")
        base = (f"https://graph.microsoft.com/v1.0/users/{user_upn}/calendarView"
                f"?startDateTime={start_dt_iso}&endDateTime={end_dt_iso}")
    else:
        base = f"https://graph.microsoft.com/v1.0/users/{user_upn}/events"

    select = (
        "$select=id,subject,createdDateTime,lastModifiedDateTime,organizer,attendees,"
        "start,end,location,isOnlineMeeting,onlineMeeting,onlineMeetingUrl,webLink"
    )
    orderby = "$orderby=start/dateTime"
    top = "$top=50"

    joiner = "&" if "?" in base else "?"
    url = f"{base}{joiner}{select}&{orderby}&{top}"

    all_events = []
    for page in graph_paged_get(url, headers):
        all_events.extend(page.get("value", []))
    return all_events

# ---------- Online meeting + attendance ----------
def find_online_meeting_by_join_url(headers: dict, user_upn: str, join_url: str) -> dict | None:
    base = f"https://graph.microsoft.com/v1.0/users/{user_upn}/onlineMeetings"
    params = {"$filter": f"JoinWebUrl eq '{join_url}'", "$top": "1"}
    for page in graph_paged_get(base, headers, params=params):
        items = page.get("value", [])
        if items:
            return items[0]
    return None

def fetch_attendance_for_meeting(headers: dict, user_upn: str, meeting_id: str) -> list:
    reports_url = f"https://graph.microsoft.com/v1.0/users/{user_upn}/onlineMeetings/{meeting_id}/attendanceReports"
    all_reports = []
    for page in graph_paged_get(reports_url, headers):
        for rep in page.get("value", []):
            rid = rep.get("id")
            records_url = (f"https://graph.microsoft.com/v1.0/users/{user_upn}/"
                           f"onlineMeetings/{meeting_id}/attendanceReports/{rid}/attendanceRecords")
            all_records = []
            for rpage in graph_paged_get(records_url, headers):
                all_records.extend(rpage.get("value", []))
            all_reports.append({
                "report_id": rid,
                "meetingStartDateTime": rep.get("meetingStartDateTime"),
                "meetingEndDateTime": rep.get("meetingEndDateTime"),
                "total_participants": rep.get("totalParticipantCount"),
                "records": all_records
            })
    return all_reports

# ---------- Main ----------
def main():
    try:
        access_token = acquire_app_token(TENANT, CLIENT_ID, CLIENT_SECRET)
        headers = {"Authorization": f"Bearer {access_token}"}

        now_utc = datetime.now(timezone.utc)
        end_iso = to_iso_z(now_utc)
        start_iso=f"1900-01-01T15:04:30"

        # Load checkpoint from Blob
        last_seen = load_checkpoint_from_blob()
        events = {}
        if last_seen is None:
            print(f"Querying events")
            # start_utc = now_utc - timedelta(days=30)
            events = fetch_all_events(
                        headers, SGA_UPN
                    )     # first run default
        else:
            start_utc = last_seen - timedelta(minutes=5) # small overlap
            start_iso = to_iso_z(start_utc)

            print(f"Querying events for {SGA_UPN} from {start_iso} to {end_iso}")

            # Use a bounded window
            events = fetch_all_events(
                headers, SGA_UPN, use_calendar_view=True,
                start_dt_iso=start_iso, end_dt_iso=end_iso
            )
            print(f"Total events fetched: {len(events)}")

        # ---- SAVE #1: events-only snapshot ----
        events_only_payload = {
            "user": SGA_UPN,
            "windowStartUtc": start_iso,
            "windowEndUtc": end_iso,
            "fetchedUtc": to_iso_z(now_utc),
            "count": len(events),
            "events": events,
        }
        events_only_url = save_json_to_blob(
            events_only_payload,
            app_prefix="msteams/events-only",
            file_name= "events"
        )
        print("Saved EVENTS-ONLY JSON to:", events_only_url)

        # ---- Enrich with attendance & track latest start ----
        enriched_events = []
        latest_start_seen = last_seen or datetime.min.replace(tzinfo=timezone.utc)
         
        for ev in events:
            ev_start_dt = parse_graph_datetime((ev.get("start") or {}).get("dateTime"))
            if ev_start_dt and ev_start_dt > latest_start_seen:
                latest_start_seen = ev_start_dt

            join    = (ev.get("onlineMeeting", {}) or {}).get("joinUrl") or ev.get("onlineMeetingUrl")
            online  = ev.get("isOnlineMeeting")

            meeting_meta = None
            attendance_payload = None
            if join and online:
                try:
                    meta = find_online_meeting_by_join_url(headers, SGA_UPN, join)
                    if meta and meta.get("id"):
                        meeting_meta = meta
                        reports = fetch_attendance_for_meeting(headers, SGA_UPN, meta["id"])
                        attendance_payload = {
                            "onlineMeetingId": meta["id"],
                            "attendanceReports": reports
                        }
                except Exception as ex:
                    attendance_payload = {"error": f"attendance lookup failed: {ex}"}

            enriched = dict(ev)
            if meeting_meta:
                enriched["onlineMeetingMeta"] = meeting_meta
            if attendance_payload:
                enriched["attendance"] = attendance_payload
            enriched_events.append(enriched)

        # ---- SAVE #2: final (events + attendance) ----
        final_json = {
            "user": SGA_UPN,
            "windowStartUtc": start_iso,
            "windowEndUtc": end_iso,
            "fetchedUtc": to_iso_z(now_utc),
            "count": len(enriched_events),
            "events": enriched_events,
        }
        final_url = save_json_to_blob(
            final_json,
            app_prefix="msteams/final-with-attendance",
            file_name= "event_attendence_details"
        )
        print("Saved FINAL (events+attendance) JSON to:", final_url)

        # ---- Update registry in Blob ----
        if latest_start_seen:
            reg_url = save_checkpoint_to_blob(latest_start_seen)
            print("Updated registry blob:", reg_url, "->", to_iso_z(latest_start_seen))
        else:
            print("No valid meeting start times found to update registry.")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
