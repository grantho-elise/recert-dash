# ─────────────────────────────────────────────────────────────────────────────
# ADD THIS AS THE LAST PYTHON CELL in the "Recertification Funnel Insights"
# Hex project (project ID: 019d78f6-2fa0-7003-98b4-8e59c1018d32).
#
# Prerequisites in Hex (Settings → Secrets):
#   GITHUB_TOKEN  — GitHub personal access token with "gist" scope
#   GIST_ID       — Leave blank on first run; Hex will print the ID to paste back
#
# This cell assumes the SQL cells above have already produced these dataframes:
#   property_df   — building breakdown (NAME, RESIDENTS_STARTED, CERTS_STARTED,
#                   IN_PROGRESS, APPROVED, DENIED)
#   weekly_df     — weekly trend (WEEK_START, RESIDENTS_STARTED, CERTS_STARTED,
#                   IN_PROGRESS, APPROVED, DENIED)
#   timing_df     — approval timing (AVG_DAYS, MEDIAN_DAYS, N)
# ─────────────────────────────────────────────────────────────────────────────

import requests
import json
from datetime import datetime, timezone, timedelta

# ── Secrets ──────────────────────────────────────────────────────────────────
GITHUB_TOKEN = get_secret("GITHUB_TOKEN")   # Hex built-in for secrets
GIST_ID      = get_secret("GIST_ID")        # blank on first run

# ── Helper: camelCase key rename ─────────────────────────────────────────────
KEY_MAP = {
    "NAME":               "name",
    "RESIDENTS_STARTED":  "residentsStarted",
    "CERTS_STARTED":      "certsStarted",
    "IN_PROGRESS":        "inProgress",
    "APPROVED":           "approved",
    "DENIED":             "denied",
}

def rename_keys(records):
    return [{KEY_MAP.get(k, k): v for k, v in row.items()} for row in records]

# ── Format property data ──────────────────────────────────────────────────────
property_records = rename_keys(property_df.to_dict(orient="records"))

# ── Format weekly data ────────────────────────────────────────────────────────
today = datetime.now(timezone.utc).date()
weekly_records = []
for row in weekly_df.to_dict(orient="records"):
    raw_date = row.get("WEEK_START")
    date_str = str(raw_date)[:10] if raw_date is not None else ""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        label = d.strftime("%b %-d")
        # Mark current week with ✦
        week_start = d.date()
        delta = (today - week_start).days
        if 0 <= delta <= 6:
            label += " ✦"
    except Exception:
        label = date_str

    weekly_records.append({
        "week":             date_str,
        "label":            label,
        "residentsStarted": int(row.get("RESIDENTS_STARTED", 0) or 0),
        "certsStarted":     int(row.get("CERTS_STARTED", 0) or 0),
        "inProgress":       int(row.get("IN_PROGRESS", 0) or 0),
        "approved":         int(row.get("APPROVED", 0) or 0),
        "denied":           int(row.get("DENIED", 0) or 0),
    })

# ── Format timing data ────────────────────────────────────────────────────────
t = timing_df.iloc[0].to_dict() if len(timing_df) > 0 else {}
timing = {
    "avgDays":    float(t["AVG_DAYS"])    if t.get("AVG_DAYS")    is not None else None,
    "medianDays": float(t["MEDIAN_DAYS"]) if t.get("MEDIAN_DAYS") is not None else None,
    "n":          int(t["N"])             if t.get("N")           is not None else 0,
}

# ── Build payload ─────────────────────────────────────────────────────────────
payload = {
    "propertyData": property_records,
    "weeklyData":   weekly_records,
    "timing":       timing,
    "refreshed_at": datetime.now(timezone.utc).isoformat(),
}

gist_content = json.dumps(payload, indent=2, default=str)

# ── Push to GitHub Gist ───────────────────────────────────────────────────────
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept":        "application/vnd.github.v3+json",
}
file_obj = {"dashboard_data.json": {"content": gist_content}}

if GIST_ID and GIST_ID.strip():
    resp = requests.patch(
        f"https://api.github.com/gists/{GIST_ID.strip()}",
        headers=headers,
        json={"files": file_obj},
    )
    print(f"✓ Updated gist (status {resp.status_code})")
else:
    resp = requests.post(
        "https://api.github.com/gists",
        headers=headers,
        json={
            "description": "Elise Recertification Dashboard Data",
            "public":      False,   # secret gist — accessible via URL, not listed publicly
            "files":       file_obj,
        },
    )
    gist_data = resp.json()
    print(f"✓ Created new gist (status {resp.status_code})")
    print(f"  Gist ID  : {gist_data['id']}")
    print(f"  Raw URL  : {gist_data['files']['dashboard_data.json']['raw_url']}")
    print()
    print("  ➜ Copy the Gist ID above, go to Hex Settings → Secrets,")
    print("    and save it as GIST_ID so future runs update the same gist.")

raw_url = resp.json()["files"]["dashboard_data.json"]["raw_url"]
print(f"\n  Dashboard data URL: {raw_url}")
print(f"  Records: {len(property_records)} buildings, {len(weekly_records)} weeks")
