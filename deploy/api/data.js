// Vercel Serverless Function — /api/data
// Fetches dashboard data from a GitHub Gist published by the Hex project.
// No Snowflake credentials needed — Hex handles the DB connection.
//
// Env vars (set in Vercel dashboard):
//   GIST_URL  — raw GitHub Gist URL, e.g.:
//               https://gist.githubusercontent.com/username/abc123/raw/dashboard_data.json
//   HEX_API_TOKEN  — (optional) Hex API token to trigger a fresh run on demand
//   HEX_PROJECT_ID — (optional) override project ID
//   Source project: https://app.hex.tech/2feafe6d-0334-4e75-b35f-caf2175e2040/thread/019d872c-79e4-711a-a84b-a5f3dda9b526

const HEX_PROJECT_ID = process.env.HEX_PROJECT_ID || '019d872c-79e4-711a-a84b-a5f3dda9b526';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();

  const gistUrl = process.env.GIST_URL;
  if (!gistUrl) {
    return res.status(503).json({
      error: 'GIST_URL not configured — add it in Vercel environment variables',
    });
  }

  // ── Optionally trigger a fresh Hex run (fire-and-forget) ─────────────────
  // If HEX_API_TOKEN is set, kick off a new run in the background.
  // The current request returns cached Gist data immediately;
  // the next refresh will have the freshly computed results.
  if (process.env.HEX_API_TOKEN) {
    fetch(`https://app.hex.tech/api/v1/projects/${HEX_PROJECT_ID}/runs`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${process.env.HEX_API_TOKEN}`,
        'Content-Type':  'application/json',
      },
      body: JSON.stringify({ inputParams: {} }),
    }).catch(() => {}); // fire-and-forget — don't block the response
  }

  // ── Fetch cached data from Gist ───────────────────────────────────────────
  try {
    // GitHub Gist raw URLs cache aggressively — bust with timestamp
    const bustUrl = `${gistUrl}?t=${Date.now()}`;
    const resp = await fetch(bustUrl, {
      headers: { 'Cache-Control': 'no-cache' },
    });

    if (!resp.ok) {
      return res.status(resp.status).json({
        error: `Failed to fetch Gist: ${resp.status} ${resp.statusText}`,
      });
    }

    const data = await resp.json();
    return res.status(200).json(data);

  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
