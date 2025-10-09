# pip install msal requests
import os, msal, requests
import sys
import json
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceExistsError
import config as cfg


TENANT       = cfg.get_details("tenant")
CLIENT_ID    = cfg.get_details("client_id")
CLIENT_SECRET= cfg.get_details("client_scret")  # keeping your existing key name
SGA_UPN      = cfg.get_details("sga_upn")


def save_json_to_blob(
    json_payload,                      # str | bytes | dict | list (we'll dump if dict/list)
    account_url="https://sgaanalyticsstorageacnt.blob.core.windows.net",
    container="staging",
    app_prefix="ms-graph-events",
    blob_name=None,
    overwrite=False
) -> str:
    # Auth + client
    cred = DefaultAzureCredential()
    svc  = BlobServiceClient(account_url=account_url, credential=cred)

    # Ensure container exists (no-op if already there)
    try:
        svc.get_container_client(container).create_container()
    except ResourceExistsError:
        pass

    # Path: app-a/YYYY/MM/DD/<UTC_ISO>.json (or use provided blob_name)
    if not blob_name:
        now = datetime.now(timezone.utc)
        ts  = now.strftime("%Y%m%dT%H%M%SZ")
        blob_name = f"{app_prefix}/{now:%Y/%m/%d}/events_{ts}.json"

    # Normalize to bytes
    if isinstance(json_payload, (dict, list)):
        data = json.dumps(json_payload, ensure_ascii=False, indent=2).encode("utf-8")
    elif isinstance(json_payload, str):
        data = json_payload.encode("utf-8")
    elif isinstance(json_payload, bytes):
        data = json_payload
    else:
        raise TypeError("json_payload must be dict, list, str, or bytes")

    # Upload
    blob = svc.get_blob_client(container, blob_name)
    blob.upload_blob(
        data=data,
        overwrite=overwrite,
        content_settings=ContentSettings(content_type="application/json"),
    )

    return f"{account_url.rstrip('/')}/{container}/{blob_name}"


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


def graph_paged_get(start_url: str, headers: dict):
    """
    Generator that follows @odata.nextLink to yield each page's JSON.
    """
    url = start_url
    while url:
        r = requests.get(url, headers=headers, timeout=60)
        try:
            r.raise_for_status()
        except Exception:
            # Show response text for easier troubleshooting
            raise RuntimeError(f"Graph GET failed {r.status_code}: {r.text}")
        payload = r.json()
        yield payload
        url = payload.get("@odata.nextLink")


def fetch_all_events(headers: dict, user_upn: str, use_calendar_view=False,
                     start_dt_iso=None, end_dt_iso=None) -> list:
    """
    Fetch all events for the user, following pagination.
    When use_calendar_view=True, you MUST pass start_dt_iso and end_dt_iso (UTC or with TZ offset).
    """
    if use_calendar_view:
        if not (start_dt_iso and end_dt_iso):
            raise ValueError("start_dt_iso and end_dt_iso are required when use_calendar_view=True")
        # Calendar view is best when you want a bounded time range.
        base = (f"https://graph.microsoft.com/v1.0/users/{user_upn}/calendarView"
                f"?startDateTime={start_dt_iso}&endDateTime={end_dt_iso}")
    else:
        # /events may be large; we rely on pagination to walk everything the app is allowed to read.
        base = f"https://graph.microsoft.com/v1.0/users/{user_upn}/events"

    # Ask Graph to give us useful fields, ordered by start time when present.
    # Note: start/end are complex types with {dateTime, timeZone}.
    select = (
        "$select=id,subject,createdDateTime,lastModifiedDateTime,organizer,attendees,"
        "start,end,location,isOnlineMeeting,onlineMeeting,onlineMeetingUrl,webLink"
    )
    orderby = "$orderby=start/dateTime"  # harmless if an event lacks start

    # Tune page size. 50 is a good balance; Graph allows up to ~1000 in some cases.
    top = "$top=50"

    # Build first page URL
    joiner = "&" if "?" in base else "?"
    url = f"{base}{joiner}{select}&{orderby}&{top}"

    all_events = []
    for page in graph_paged_get(url, headers):
        values = page.get("value", [])
        all_events.extend(values)

    return all_events


def main():
    try:
        # ---- Acquire application token ----
        access_token = acquire_app_token(TENANT, CLIENT_ID, CLIENT_SECRET)
        headers = {"Authorization": f"Bearer {access_token}"}

        print("Got token. Querying events for:", SGA_UPN)

        # Option A: fetch ALL events (paged) the app can see
        events = fetch_all_events(headers, SGA_UPN)

        # # Option B (bounded window): uncomment and set your window (ISO8601 with timezone or Z)
        # start_dt_iso = "2025-10-01T00:00:00Z"
        # end_dt_iso   = "2025-10-31T23:59:59Z"
        # events = fetch_all_events(headers, SGA_UPN, use_calendar_view=True,
        #                           start_dt_iso=start_dt_iso, end_dt_iso=end_dt_iso)

        print(f"Total events fetched: {len(events)}")

        # ---- Iterate through all events (example prints; adjust as needed) ----
        for idx, ev in enumerate(events, start=1):
            ev_id   = ev.get("id")
            subject = ev.get("subject")
            start   = ev.get("start", {}).get("dateTime")
            end     = ev.get("end", {}).get("dateTime")
            tz      = ev.get("start", {}).get("timeZone")
            online  = ev.get("isOnlineMeeting")
            join    = (ev.get("onlineMeeting", {}) or {}).get("joinUrl") or ev.get("onlineMeetingUrl")
            web     = ev.get("webLink")

            print(f"[{idx}] {subject!r} | {start} -> {end} ({tz}) | Online: {online} | Join: {join or '-'} | Web: {web or '-'} | ID: {ev_id}")

        # ---- Save everything to blob ----
        final_json = {
            "user": SGA_UPN,
            "fetchedUtc": datetime.now(timezone.utc).isoformat(),
            "count": len(events),
            "events": events,
        }
        blob_url = save_json_to_blob(final_json)
        print("Saved events JSON to:", blob_url)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
