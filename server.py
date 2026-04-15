#!/usr/bin/env python3
"""
Recertification Dashboard - Local Refresh Server
================================================
Proxies LogRocket session data AND Snowflake application data so the
dashboard's "Refresh Data" button can fetch live data without CORS issues.

SETUP (one time):
  1. Install dependencies:
       pip install snowflake-connector-python

  2. Set env vars (or edit the config block below):
       export LR_TOKEN="your_logrocket_api_token"
       export SF_ACCOUNT="your_account.snowflakecomputing.com"
       export SF_USER="your_username"
       export SF_PASSWORD="your_password"

  3. Run:
       python server.py

  The dashboard will automatically use this server when it's running.
  Open http://localhost:8765/ in your browser.
"""

import http.server
import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────────────
PORT = 8765

# LogRocket
LR_TOKEN      = os.environ.get("LR_TOKEN", "YOUR_LOGROCKET_API_TOKEN_HERE")
LR_ORG        = "0qt8h9"
LR_PROJECT    = "applications-portal"
LR_SEGMENT_ID = "1307648"

# Snowflake
SF_ACCOUNT   = os.environ.get("SF_ACCOUNT",   "")   # e.g. xy12345.us-east-1
SF_USER      = os.environ.get("SF_USER",      "")
SF_PASSWORD  = os.environ.get("SF_PASSWORD",  "")
SF_WAREHOUSE = os.environ.get("SF_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = os.environ.get("SF_DATABASE",  "ELISE")
SF_SCHEMA    = os.environ.get("SF_SCHEMA",    "DA")
SF_ROLE      = os.environ.get("SF_ROLE",      "")

# Emails/domains to exclude from LogRocket sessions (internal / test accounts)
EXCLUDE_DOMAINS = ("@meetelise", "@eliseai", "chrome_headless", "headless")

# ── Snowflake ─────────────────────────────────────────────────────────────────
try:
    import snowflake.connector
    _SF_AVAILABLE = True
except ImportError:
    _SF_AVAILABLE = False


def _sf_configured():
    return _SF_AVAILABLE and bool(SF_ACCOUNT) and bool(SF_USER) and bool(SF_PASSWORD)


def _sf_connect():
    kwargs = dict(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )
    if SF_ROLE:
        kwargs["role"] = SF_ROLE
    return snowflake.connector.connect(**kwargs)


# SQL — all queries join FCT_APPLICATIONS → DIM_BUILDINGS, IS_TEST=FALSE
_SQL_PROPERTY = """
SELECT
    b.BUILDING_NAME || ' (' || b.ORG_NAME || ')' AS name,
    COUNT(*)                                                                     AS residents_started,
    SUM(CASE WHEN a.STATUS IN ('InReview','Approved') THEN 1 ELSE 0 END)        AS certs_started,
    SUM(CASE WHEN a.STATUS = 'Pending'                THEN 1 ELSE 0 END)        AS in_progress,
    SUM(CASE WHEN a.STATUS = 'Approved'               THEN 1 ELSE 0 END)        AS approved,
    SUM(CASE WHEN a.STATUS IN ('Cancelled','Denied')  THEN 1 ELSE 0 END)        AS denied
FROM ELISE.DA.FCT_APPLICATIONS a
JOIN ELISE.DA.DIM_BUILDINGS    b ON a.BUILDING_ID = b.ID
WHERE a.APPLICATION_TYPE = 'AffordableRecertification'
  AND b.IS_TEST = FALSE
GROUP BY b.BUILDING_NAME, b.ORG_NAME
ORDER BY residents_started DESC
"""

_SQL_WEEKLY = """
SELECT
    DATE_TRUNC('week', a.TIME_CREATED)::date                                     AS week_start,
    COUNT(*)                                                                     AS residents_started,
    SUM(CASE WHEN a.STATUS IN ('InReview','Approved') THEN 1 ELSE 0 END)        AS certs_started,
    SUM(CASE WHEN a.STATUS = 'Pending'                THEN 1 ELSE 0 END)        AS in_progress,
    SUM(CASE WHEN a.STATUS = 'Approved'               THEN 1 ELSE 0 END)        AS approved,
    SUM(CASE WHEN a.STATUS IN ('Cancelled','Denied')  THEN 1 ELSE 0 END)        AS denied
FROM ELISE.DA.FCT_APPLICATIONS a
JOIN ELISE.DA.DIM_BUILDINGS    b ON a.BUILDING_ID = b.ID
WHERE a.APPLICATION_TYPE = 'AffordableRecertification'
  AND b.IS_TEST = FALSE
GROUP BY DATE_TRUNC('week', a.TIME_CREATED)
ORDER BY week_start
"""

_SQL_TIMING = """
SELECT
    ROUND(AVG(DATEDIFF('day', a.TIME_CREATED, a.APPLICATION_DECISION_TIME)), 1)    AS avg_days,
    ROUND(MEDIAN(DATEDIFF('day', a.TIME_CREATED, a.APPLICATION_DECISION_TIME)), 1) AS median_days,
    COUNT(*)                                                                        AS n
FROM ELISE.DA.FCT_APPLICATIONS a
JOIN ELISE.DA.DIM_BUILDINGS    b ON a.BUILDING_ID = b.ID
WHERE a.APPLICATION_TYPE = 'AffordableRecertification'
  AND b.IS_TEST = FALSE
  AND a.STATUS = 'Approved'
  AND a.APPLICATION_DECISION_TIME IS NOT NULL
"""


def _fmt_week_label(week_date):
    """Format a date object as 'Mar 16' style label."""
    try:
        return week_date.strftime("%b %-d")
    except ValueError:
        return week_date.strftime("%b %d").lstrip("0")


def fetch_snowflake_data():
    """Run all three Snowflake queries and return structured dashboard data."""
    if not _sf_configured():
        missing = []
        if not _SF_AVAILABLE:
            missing.append("snowflake-connector-python not installed — run: pip install snowflake-connector-python")
        if not SF_ACCOUNT:
            missing.append("SF_ACCOUNT not set")
        if not SF_USER:
            missing.append("SF_USER not set")
        if not SF_PASSWORD:
            missing.append("SF_PASSWORD not set")
        return {"error": "Snowflake not configured: " + "; ".join(missing)}

    try:
        conn = _sf_connect()
        cur  = conn.cursor()

        # 1) Property breakdown
        cur.execute(_SQL_PROPERTY)
        property_data = [
            {
                "name":             row[0],
                "residentsStarted": int(row[1]),
                "certsStarted":     int(row[2]),
                "inProgress":       int(row[3]),
                "approved":         int(row[4]),
                "denied":           int(row[5]),
            }
            for row in cur.fetchall()
        ]

        # 2) Weekly trend
        today = datetime.now(timezone.utc).date()
        cur.execute(_SQL_WEEKLY)
        weekly_data = []
        for row in cur.fetchall():
            week_date = row[0]          # date object from Snowflake
            week_str  = week_date.isoformat()
            label     = _fmt_week_label(week_date)
            delta = (today - week_date).days
            if 0 <= delta <= 6:
                label += " ✦"           # mark current week
            weekly_data.append({
                "week":             week_str,
                "label":            label,
                "residentsStarted": int(row[1]),
                "certsStarted":     int(row[2]),
                "inProgress":       int(row[3]),
                "approved":         int(row[4]),
                "denied":           int(row[5]),
            })

        # 3) Timing metrics
        cur.execute(_SQL_TIMING)
        timing_row = cur.fetchone()
        timing = {
            "avgDays":    float(timing_row[0]) if timing_row and timing_row[0] is not None else None,
            "medianDays": float(timing_row[1]) if timing_row and timing_row[1] is not None else None,
            "n":          int(timing_row[2])   if timing_row and timing_row[2] is not None else 0,
        }

        cur.close()
        conn.close()

        return {
            "propertyData": property_data,
            "weeklyData":   weekly_data,
            "timing":       timing,
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        return {"error": str(e)}


# ── LogRocket ─────────────────────────────────────────────────────────────────
def logrocket_request(path, params=None):
    url = f"https://api.logrocket.com/v1/{path}"
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{query}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {LR_TOKEN}",
            "Content-Type": "application/json",
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"error": str(e), "status": e.code}
    except Exception as e:
        return {"error": str(e)}


def fetch_sessions():
    """Fetch sessions from LogRocket API, filtered for real users."""
    data = logrocket_request(
        f"organizations/{LR_ORG}/projects/{LR_PROJECT}/recordings",
        params={"timeRange": "1M", "limit": "50"},
    )

    recordings = data.get("recordings", data.get("data", []))
    if not recordings and "error" not in data:
        recordings = data if isinstance(data, list) else []

    sessions = []
    for rec in recordings:
        user  = rec.get("user", {}) or {}
        name  = user.get("name") or user.get("displayName") or "Unknown"
        email = user.get("email") or ""

        email_lower = email.lower()
        if any(excl in email_lower for excl in EXCLUDE_DOMAINS):
            continue
        if not email:
            continue

        recording_id = rec.get("id") or rec.get("recordingId") or ""
        session_url  = f"https://app.logrocket.com/{LR_ORG}/{LR_PROJECT}/s/{recording_id}/0"
        if rec.get("url"):
            session_url = rec["url"]

        created = rec.get("createdAt") or rec.get("timestamp") or ""
        try:
            dt       = datetime.fromisoformat(created.replace("Z", "+00:00"))
            date_str = dt.strftime("%b %-d, %Y %-I:%M %p")
        except Exception:
            date_str = created[:16] if created else ""

        ua          = rec.get("userAgent") or rec.get("browser") or {}
        browser     = ua.get("browser", {})
        os_info     = ua.get("os", {})
        browser_str = f"{browser.get('name', '')} {browser.get('version', '')}".strip()
        os_str      = f"{os_info.get('name', '')} {os_info.get('version', '')}".strip()
        device_str  = ua.get("device", {}).get("type", "Desktop").capitalize()

        sessions.append({
            "name":    name,
            "email":   email,
            "url":     session_url,
            "date":    date_str,
            "browser": browser_str or "Unknown",
            "os":      os_str or "Unknown",
            "device":  device_str or "Desktop",
        })

    return sessions[:15]


# ── Request Handler ───────────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):

    def send_json(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path, content_type):
        try:
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.end_headers()

    def do_GET(self):
        ts = datetime.now().strftime("%H:%M:%S")

        # ── /api/data — Snowflake property + weekly + timing data ──
        if self.path == "/api/data":
            print(f"[{ts}] Fetching Snowflake data...")
            result = fetch_snowflake_data()
            if "error" in result:
                print(f"  ✗ {result['error']}")
                self.send_json(result, status=503)
            else:
                props = len(result.get("propertyData", []))
                weeks = len(result.get("weeklyData", []))
                print(f"  → {props} buildings, {weeks} weeks")
                self.send_json(result)

        # ── /api/sessions — LogRocket session recordings ──
        elif self.path == "/api/sessions":
            print(f"[{ts}] Fetching LogRocket sessions...")
            if "YOUR_LOGROCKET_API_TOKEN_HERE" in LR_TOKEN:
                self.send_json({
                    "error": "LogRocket API token not configured. Set LR_TOKEN env var.",
                    "sessions": [],
                    "refreshed_at": datetime.now(timezone.utc).isoformat(),
                }, status=503)
                return
            sessions = fetch_sessions()
            print(f"  → {len(sessions)} sessions returned")
            self.send_json({
                "sessions":     sessions,
                "count":        len(sessions),
                "segment_id":   LR_SEGMENT_ID,
                "refreshed_at": datetime.now(timezone.utc).isoformat(),
            })

        # ── / — Serve the dashboard HTML ──
        elif self.path in ("/", "/index.html", "/recertification_dashboard.html"):
            self.send_file("recertification_dashboard.html", "text/html; charset=utf-8")

        elif self.path == "/health":
            self.send_json({
                "status":    "ok",
                "port":      PORT,
                "snowflake": "configured" if _sf_configured() else "not configured",
                "logrocket": "configured" if "YOUR_LOGROCKET" not in LR_TOKEN else "not configured",
            })

        else:
            self.send_error(404)

    def log_message(self, fmt, *args):
        pass  # suppress default access log noise


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sf_status = (
        "✓ configured" if _sf_configured()
        else ("✗ NOT SET — set SF_ACCOUNT, SF_USER, SF_PASSWORD" if _SF_AVAILABLE
              else "✗ snowflake-connector-python not installed  →  pip install snowflake-connector-python")
    )
    lr_status = "✓ configured" if "YOUR_LOGROCKET" not in LR_TOKEN else "✗ NOT SET — set LR_TOKEN env var"

    server = http.server.HTTPServer(("localhost", PORT), Handler)
    print(f"""
╔══════════════════════════════════════════════════════╗
║   Recertification Dashboard - Refresh Server         ║
╚══════════════════════════════════════════════════════╝

  Dashboard:  http://localhost:{PORT}/
  Endpoints:
    /api/data      → Snowflake (buildings + weekly trend + timing)
    /api/sessions  → LogRocket session recordings
    /health        → Config status

  Snowflake: {sf_status}
  LogRocket: {lr_status}

  Open the dashboard at http://localhost:{PORT}/ in your browser.
  Click "Refresh Data" to pull live Snowflake + LogRocket data.

  Press Ctrl+C to stop.
""")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
