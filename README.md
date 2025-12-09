# MS Teams Container App Job (`ms-team-container-app-job`)

This project contains the **Microsoft Teams attendance extract job** used in the SGA analytics platform.

It runs as an **Azure Container Apps job** (or similar container runtime) and is orchestrated from Azure Synapse to:

- Call **Microsoft Graph** for a small set of SGA user accounts.
- Retrieve **calendar events** and, for online meetings, **attendance reports and records**.
- Land **JSON snapshots** and **flattened Parquet tables** into Azure Blob Storage.
- Maintain a **checkpoint** (latest meeting start time) so that each run is **incremental**.

The Parquet outputs are read by Synapse External Tables and loaded into the dedicated SQL pool for downstream transformations and reporting.

---

## 1. High-Level Flow

1. The container job starts and reads configuration from environment variables (`tenant`, `client_id`, `client_screte`, `SGA_UPN*`, etc.).
2. It acquires an **application token** for Microsoft Graph via **MSAL** (`client_credentials` flow).
3. It loads a **checkpoint** (latest meeting start timestamp) from a Blob registry file.
4. For each configured SGA account (e.g. `sgacommittees`, `mstedman`, `sgatraining`):
   - Fetches meetings (events) from the user’s calendar.
   - Saves an **“events-only” JSON** snapshot.
   - For online meetings, looks up the **onlineMeeting** object and fetches **attendance reports + records**.
   - Saves a **“final with attendance” JSON** snapshot.
   - Flattens data into three **Parquet tables**:
     - Events
     - Attendance reports
     - Attendance records
5. After all users are processed, it updates the **checkpoint** in Blob, so the next run only processes newer meetings.
6. Synapse pipelines use **External Tables** over the Parquet files to load data into the Synapse dedicated SQL pool.

---

## 2. Project Structure

Key files:

- **`attendance_reports_main.py`**  
  Main entry point and orchestration logic:

  - Reads tenant, client ID/secret and SGA UPNs from `config.py`.
  - Obtains a Graph access token using **MSAL** (`ConfidentialClientApplication`).
  - Loads the last run checkpoint from Blob (`load_checkpoint_from_blob()`).
  - Decides the time window:
    - First run → last 30 days.
    - Subsequent runs → from `last_checkpoint - 5 minutes` to now.
  - Fetches events for each SGA user (`fetch_all_events()`).
  - Saves **events-only JSON** using `save_json_to_blob(...)`.
  - For each online meeting event:
    - Resolves the Graph **onlineMeeting** by join URL (`find_online_meeting_by_join_url()`).
    - Fetches attendance reports and records for that meeting (`fetch_attendance_for_meeting()`).
    - Embeds `onlineMeetingMeta` and `attendance` into the event payload.
  - Saves **final JSON with attendance** using `save_json_to_blob(...)`.
  - Converts combined JSON docs to DataFrames using `json_docs_to_dataframes(...)` from `parquet_utils.py`.
  - Writes Parquet tables to Blob using `write_parquet_blob(...)`.
  - Updates the checkpoint via `save_checkpoint_to_blob()` and exits with status code 0/1.

- **`config.py`**  
  Simple configuration helper that reads environment variables:

  - `tenant` – Azure AD tenant ID.  
  - `client_id` – App registration client ID.  
  - `client_screte` – App registration client secret (note spelling in code).  
  - `SGA_UPN`, `SGA_UPN2`, `SGA_UPN3` – UPNs of the SGA accounts whose calendars/meetings are processed.

  Provides a helper:

  ```python
  def get_details(key=""):
      return {
          "tenant": tenant,
          "client_id": client_id,
          "client_scret": client_screte,
          "sga_upn": sga_upn,
          "sga_upn2": sga_upn2,
          "sga_upn3": sga_upn3,
      }[key]
