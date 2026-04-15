// Vercel Serverless Function — /api/sessions
// Fetches recent LogRocket session recordings, filters internal/test users
// Env vars: LR_TOKEN

const LR_ORG     = '0qt8h9';
const LR_PROJECT = 'applications-portal';
const SEGMENT_ID = '1307648';
const EXCLUDE    = ['@meetelise', '@eliseai', 'chrome_headless', 'headless'];

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(204).end();

  const token = process.env.LR_TOKEN;
  if (!token) {
    return res.status(503).json({
      error: 'LogRocket token not configured — set LR_TOKEN in Vercel env vars',
      sessions: [],
    });
  }

  try {
    const url = `https://api.logrocket.com/v1/organizations/${LR_ORG}/projects/${LR_PROJECT}/recordings?timeRange=1M&limit=50`;
    const resp = await fetch(url, {
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    });

    if (!resp.ok) {
      return res.status(resp.status).json({ error: `LogRocket API error: ${resp.status}`, sessions: [] });
    }

    const data = await resp.json();
    let recordings = data.recordings || data.data || (Array.isArray(data) ? data : []);

    const sessions = [];
    for (const rec of recordings) {
      const user  = rec.user || {};
      const name  = user.name || user.displayName || 'Unknown';
      const email = user.email || '';

      const el = email.toLowerCase();
      if (!email || EXCLUDE.some(x => el.includes(x))) continue;

      const id  = rec.id || rec.recordingId || '';
      const url = rec.url || `https://app.logrocket.com/${LR_ORG}/${LR_PROJECT}/s/${id}/0`;

      const created = rec.createdAt || rec.timestamp || '';
      let date = '';
      try {
        date = new Date(created).toLocaleString('en-US', {
          month: 'short', day: 'numeric', year: 'numeric',
          hour: 'numeric', minute: '2-digit', hour12: true,
        });
      } catch (_) { date = created.slice(0, 16); }

      const ua      = rec.userAgent || rec.browser || {};
      const browser = ua.browser || {};
      const osInfo  = ua.os || {};
      const device  = (ua.device || {}).type || 'Desktop';

      sessions.push({
        name,
        email,
        url,
        date,
        browser: [browser.name, browser.version].filter(Boolean).join(' ') || 'Unknown',
        os:      [osInfo.name, osInfo.version].filter(Boolean).join(' ') || 'Unknown',
        device:  device.charAt(0).toUpperCase() + device.slice(1),
      });

      if (sessions.length >= 15) break;
    }

    return res.status(200).json({
      sessions,
      count:        sessions.length,
      segment_id:   SEGMENT_ID,
      refreshed_at: new Date().toISOString(),
    });

  } catch (err) {
    console.error('LogRocket error:', err.message);
    return res.status(500).json({ error: err.message, sessions: [] });
  }
};
