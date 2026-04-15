// Vercel Serverless Function — /api/data
// Queries Snowflake: property breakdown, weekly trend, timing metrics
// Env vars: SF_ACCOUNT, SF_USER, SF_PASSWORD, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE

const snowflake = require('snowflake-sdk');

snowflake.configure({ logLevel: 'ERROR' });

const SF = {
  account:   process.env.SF_ACCOUNT,
  username:  process.env.SF_USER,
  password:  process.env.SF_PASSWORD,
  warehouse: process.env.SF_WAREHOUSE || 'COMPUTE_WH',
  database:  process.env.SF_DATABASE  || 'ELISE',
  schema:    process.env.SF_SCHEMA    || 'DA',
  role:      process.env.SF_ROLE      || undefined,
};

const SQL_PROPERTY = `
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
  ORDER BY residents_started DESC`;

const SQL_WEEKLY = `
  SELECT
    DATE_TRUNC('week', a.TIME_CREATED)::date AS week_start,
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
  ORDER BY week_start`;

const SQL_TIMING = `
  SELECT
    ROUND(AVG(DATEDIFF('day', a.TIME_CREATED, a.APPLICATION_DECISION_TIME)), 1)    AS avg_days,
    ROUND(MEDIAN(DATEDIFF('day', a.TIME_CREATED, a.APPLICATION_DECISION_TIME)), 1) AS median_days,
    COUNT(*) AS n
  FROM ELISE.DA.FCT_APPLICATIONS a
  JOIN ELISE.DA.DIM_BUILDINGS    b ON a.BUILDING_ID = b.ID
  WHERE a.APPLICATION_TYPE = 'AffordableRecertification'
    AND b.IS_TEST = FALSE
    AND a.STATUS = 'Approved'
    AND a.APPLICATION_DECISION_TIME IS NOT NULL`;

function runQuery(conn, sql) {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText: sql,
      complete: (err, _stmt, rows) => err ? reject(err) : resolve(rows),
    });
  });
}

function connectSF() {
  return new Promise((resolve, reject) => {
    const conn = snowflake.createConnection(SF);
    conn.connect((err, c) => err ? reject(err) : resolve(c));
  });
}

function fmtWeekLabel(dateStr) {
  const d = new Date(dateStr + 'T12:00:00Z');
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}

function isCurrentWeek(dateStr) {
  const weekStart = new Date(dateStr + 'T00:00:00Z');
  const today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  const diff = Math.floor((today - weekStart) / 86400000);
  return diff >= 0 && diff <= 6;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') return res.status(204).end();

  if (!SF.account || !SF.username || !SF.password) {
    return res.status(503).json({
      error: 'Snowflake not configured — set SF_ACCOUNT, SF_USER, SF_PASSWORD in Vercel env vars',
    });
  }

  let conn;
  try {
    conn = await connectSF();

    const [propRows, weekRows, timingRows] = await Promise.all([
      runQuery(conn, SQL_PROPERTY),
      runQuery(conn, SQL_WEEKLY),
      runQuery(conn, SQL_TIMING),
    ]);

    const propertyData = propRows.map(r => ({
      name:             r.NAME,
      residentsStarted: Number(r.RESIDENTS_STARTED),
      certsStarted:     Number(r.CERTS_STARTED),
      inProgress:       Number(r.IN_PROGRESS),
      approved:         Number(r.APPROVED),
      denied:           Number(r.DENIED),
    }));

    const weeklyData = weekRows.map(r => {
      const dateStr = r.WEEK_START instanceof Date
        ? r.WEEK_START.toISOString().slice(0, 10)
        : String(r.WEEK_START).slice(0, 10);
      let label = fmtWeekLabel(dateStr);
      if (isCurrentWeek(dateStr)) label += ' ✦';
      return {
        week:             dateStr,
        label,
        residentsStarted: Number(r.RESIDENTS_STARTED),
        certsStarted:     Number(r.CERTS_STARTED),
        inProgress:       Number(r.IN_PROGRESS),
        approved:         Number(r.APPROVED),
        denied:           Number(r.DENIED),
      };
    });

    const t = timingRows[0] || {};
    const timing = {
      avgDays:    t.AVG_DAYS    != null ? Number(t.AVG_DAYS)    : null,
      medianDays: t.MEDIAN_DAYS != null ? Number(t.MEDIAN_DAYS) : null,
      n:          t.N           != null ? Number(t.N)           : 0,
    };

    conn.destroy(() => {});

    return res.status(200).json({
      propertyData,
      weeklyData,
      timing,
      refreshed_at: new Date().toISOString(),
    });

  } catch (err) {
    if (conn) conn.destroy(() => {});
    console.error('Snowflake error:', err.message);
    return res.status(500).json({ error: err.message });
  }
};
