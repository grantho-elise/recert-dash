// Vercel Serverless Function — /api/refresh
// Triggers the Hex project, polls until the run completes, then returns
// the freshly-written Gist data.  Called by the dashboard's "Refresh Data" button.
//
// Env vars (set in Vercel dashboard):
//   HEX_API_TOKEN  — Hex API token
//   GIST_URL       — raw GitHub Gist URL for dashboard_data.json

const HEX_PROJECT_ID = '019d872c-79e4-711a-a84b-a5f3dda9b526';
const POLL_MS        = 4000;   // poll interval
const DEADLINE_MS    = 55000;  // stay under Vercel's 60 s function limit

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(204).end();

  const hexToken = process.env.HEX_API_TOKEN;
  const gistUrl  = process.env.GIST_URL;

  if (!hexToken) return res.status(503).json({ error: 'HEX_API_TOKEN not configured — add it in Vercel environment variables' });
  if (!gistUrl)  return res.status(503).json({ error: 'GIST_URL not configured — add it in Vercel environment variables' });

  // ── 1. Trigger Hex run ────────────────────────────────────────────────────
  let runId;
  try {
    const triggerResp = await fetch(
      `https://app.hex.tech/api/v1/projects/${HEX_PROJECT_ID}/runs`,
      {
        method: 'POST',
        headers: {
          Authorization:  `Bearer ${hexToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ inputParams: {} }),
      },
    );

    if (!triggerResp.ok) {
      const e = await triggerResp.json().catch(() => ({}));
      return res.status(502).json({
        error: `Hex trigger failed: ${e.message || triggerResp.status}`,
      });
    }

    const triggerData = await triggerResp.json();
    runId = triggerData.runId || triggerData.run_id;
    if (!runId) return res.status(502).json({ error: 'Hex did not return a runId' });

  } catch (err) {
    return res.status(502).json({ error: `Hex trigger error: ${err.message}` });
  }

  // ── 2. Poll run status until complete ────────────────────────────────────
  const deadline = Date.now() + DEADLINE_MS;

  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, POLL_MS));

    try {
      const statusResp = await fetch(
        `https://app.hex.tech/api/v1/projects/${HEX_PROJECT_ID}/runs/${runId}`,
        { headers: { Authorization: `Bearer ${hexToken}` } },
      );
      if (!statusResp.ok) continue; // transient error — keep polling

      const statusData = await statusResp.json();
      const status = statusData.status || statusData.run_status || '';

      if (status === 'COMPLETED') {
        // ── 3. Fetch freshly-written Gist data ───────────────────────────
        const bustUrl  = `${gistUrl}?t=${Date.now()}`;
        const gistResp = await fetch(bustUrl, { headers: { 'Cache-Control': 'no-cache' } });

        if (!gistResp.ok) {
          return res.status(502).json({
            error: `Hex run completed but Gist fetch failed: ${gistResp.status}`,
          });
        }

        const data = await gistResp.json();
        return res.status(200).json(data);
      }

      if (['ERRORED', 'KILLED', 'UNABLE_TO_ALLOCATE_KERNEL'].includes(status)) {
        return res.status(500).json({ error: `Hex run ${status.toLowerCase()}` });
      }

      // PENDING or RUNNING — keep polling

    } catch (_) {
      // transient poll error — keep trying
    }
  }

  return res.status(504).json({
    error: 'Hex run is taking longer than expected — try Refresh again in a moment',
  });
};
