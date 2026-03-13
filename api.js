const express = require("express");
const cors    = require("cors");
const sqlite3 = require("sqlite3").verbose();
const path    = require("path");

const app  = express();
const PORT = process.env.PORT || 3001;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "users.db");

app.use(cors());
app.use(express.json());

// ── DB ────────────────────────────────────────────────────────────────────────
const db = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READONLY, (err) => {
  if (err) { console.error("❌ DB error:", err.message); process.exit(1); }
  console.log(`✅ Connected to ${DB_PATH}`);
});

function query(sql, params = []) {
  return new Promise((res, rej) =>
    db.all(sql, params, (err, rows) => err ? rej(err) : res(rows))
  );
}

// ── HELPERS ───────────────────────────────────────────────────────────────────
function safeJSON(str, fallback = null) {
  if (!str) return fallback;
  try { return JSON.parse(str); } catch { return fallback; }
}

function deriveActivityScore(r) {
  const days     = Math.min((r.active_days_365 || 0) / 365 * 50, 50);
  const contribs = Math.min((r.contrib_365d    || 0) / 1000 * 50, 50);
  return Math.round(days + contribs);
}

function deriveImpactScore(r) {
  const stars  = Math.min(Math.log10((r.star_count     || 0) + 1) / 5 * 50, 50);
  const forks  = Math.min(Math.log10((r.forks_received || 0) + 1) / 4 * 30, 30);
  const follow = Math.min(Math.log10((r.follower_count || 0) + 1) / 4 * 20, 20);
  return Math.round(stars + forks + follow);
}

function deriveSeniorityScore(r) {
  const merge  = Math.min((r.pr_merge_rate   || 0) * 0.4, 40);
  const review = Math.min((r.code_review_pct || 0) * 0.5, 20);
  const ext    = Math.min(Math.log10((r.external_prs || 0) + 1) / 3 * 20, 20);
  const orgs   = Math.min((r.org_count        || 0) * 2, 10);
  const tenure = Math.min(
    Math.floor((Date.now() - new Date(r.github_created_at || Date.now()).getTime()) / (1000*60*60*24*365)) * 1.5,
    10
  );
  return Math.round(merge + review + ext + orgs + tenure);
}

function deriveBadges(r) {
  const badges = [];
  if ((r.commit_pct      || 0) > 60) badges.push("Builder");
  if ((r.code_review_pct || 0) > 15) badges.push("Reviewer");
  if ((r.external_prs    || 0) > 20) badges.push("Collaborator");
  return badges;
}

function deriveImpactBadges(r) {
  const badges = [];
  if ((r.follower_count || 0) > 1000) badges.push("OSS Influencer");
  if ((r.star_count || 0) / Math.max(r.original_repo_count || 1, 1) > 100) badges.push("High Signal Per Project");
  return badges;
}

function deriveSeniorityBadges(r, tenureYears) {
  const badges = [];
  if (tenureYears >= 6 && (r.pr_merge_rate || 0) > 85) badges.push("Senior Signal");
  if ((r.org_count || 0) >= 3) badges.push("Enterprise Engineer");
  if (tenureYears < 3) badges.push("Emerging Talent");
  return badges;
}

// ── TRANSFORM: raw DB row → Developer shape ───────────────────────────────────
function toDevShape(row) {
  const spotlight   = safeJSON(row.spotlight_repo_json);
  const top10       = safeJSON(row.top10_repos_json, []);
  const alltimeLang = safeJSON(row.alltime_languages_json, {});
  const recentLang  = safeJSON(row.recent_languages_json, {});

  const alltimeLangs = Object.entries(alltimeLang)
    .sort((a, b) => b[1] - a[1]).slice(0, 6)
    .map(([name, pct], i) => ({ name, pct: Math.round(pct), starred: i < 2 }));

  const recentLangs = Object.entries(recentLang)
    .sort((a, b) => b[1] - a[1]).slice(0, 6)
    .map(([name, pct], i) => ({ name, pct: Math.round(pct), starred: i < 2 }));

  const alltimeTags = [
    row.alltime_primary_tag,
    row.alltime_secondary_tag,
    row.alltime_third_tag,
  ].filter(Boolean);

  const recentTagsList = [
    row.recent_primary_tag,
    row.recent_secondary_tag,
    row.recent_third_tag,
  ].filter(Boolean);

  const trend = [
    { period: "30d",  value: row.contrib_30d  || 0 },
    { period: "90d",  value: row.contrib_90d  || 0 },
    { period: "180d", value: row.contrib_180d || 0 },
    { period: "365d", value: row.contrib_365d || 0 },
  ];

  const tenureYears = row.github_created_at
    ? Math.floor((Date.now() - new Date(row.github_created_at).getTime()) / (1000*60*60*24*365))
    : 0;

  const activityScore  = row.activity_score  ?? deriveActivityScore(row);
  const impactScore    = row.impact_score     ?? deriveImpactScore(row);
  const seniorityScore = row.seniority_score  ?? deriveSeniorityScore(row);

  const hasCI      = (row.repos_with_ci      || 0) > 0;
  const hasTests   = (row.repos_with_tests   || 0) > 0;
  const hasDocker  = top10.some(r => r.has_ci);
  const hasLicense = (row.repos_with_license || 0) > 0;

  const ps = spotlight || {};
  const projectSpotlight = {
    repoName:    ps.name        || "",
    repoUrl:     ps.url         || "",
    stars:       ps.stars       || 0,
    forks:       ps.forks       || 0,
    watchers:    ps.watchers    || 0,
    lastUpdate:  ps.pushed_at   ? ps.pushed_at.split("T")[0] : "",
    description: ps.description || "",
  };

  return {
    id:                `CP-${String(row.id).padStart(6, "0")}`,
    name:              row.full_name || row.username,
    company:           row.company   || "",
    role:              row.alltime_primary_tag || "",
    location:          row.city      || "",
    blog:              row.blog_url  || "",
    linkedin:          row.linkedin_url || "",
    twitter:           row.twitter_url  || "",
    twitterFollowers:  row.twitter_follower_count || 0,
    githubUsername:    row.username,
    githubTenureYears: tenureYears,
    avatarUrl:         `https://github.com/${row.username}.png?size=80`,
    email:             row.public_email || "",
    bio:               row.bio || "",

    // ── AI Fields ────────────────────────────────────────────────────────────
    ai: {
      overview:             row.ai_overview           || null,
      overviewCollab:       row.ai_overview_collab    || null,
      standoutSignals:      [
        row.standout_1,
        row.standout_2,
        row.standout_3,
        row.standout_4,
      ].filter(Boolean),
      risks:                [
        row.risk_1,
        row.risk_2,
        row.risk_3,
      ].filter(Boolean),
      collaborationScore:   row.collaboration_score   ?? null,
      collaborationSummary: row.collaboration_summary || null,
      collaborationTags:    row.collaboration_tags ? row.collaboration_tags.split(", ").filter(Boolean) : [],
      shortlistTags:        row.shortlist_tags     ? row.shortlist_tags.split(", ").filter(Boolean)     : [],
      seniority:            row.ai_seniority          || null,
      shortlistScore:       row.ai_shortlist_score     ?? null,
      processedAt:          row.ai_processed_at        || null,
    },

    // ── Status ───────────────────────────────────────────────────────────────
    activityStatus:    row.activity_status || null,   // Actively Coding / Recently Coding / Inactive
    momentum:          row.momentum        || null,   // Accelerating / Stable / Declining

    footprint: {
      totalStars:    row.star_count          || 0,
      followers:     row.follower_count      || 0,
      originalRepos: row.original_repo_count || 0,
      repoCount:     row.repo_count          || 0,
      forkedRepos:   row.forked_repo_count   || 0,
      forksReceived: row.forks_received      || 0,
    },

    languages: {
      allTime: alltimeLangs.length ? alltimeLangs : [{ name: "Unknown", pct: 100, starred: false }],
      recent:  recentLangs.length  ? recentLangs  : alltimeLangs.slice(0, 3),
    },

    skillTags: {
      allTime: alltimeTags,
      recent:  recentTagsList,
    },

    activity: {
      score:            activityScore,
      activeDays365:    row.active_days_365 || 0,
      contributions90d: row.contrib_90d     || 0,
      momentum:         row.momentum        || "Stable",
      trend,
    },

    contributionStyle: {
      commits: row.commit_pct      || 0,
      prs:     row.pr_pct          || 0,
      reviews: row.code_review_pct || 0,
      issues:  row.issue_pct       || 0,
      badges:  deriveBadges(row),
    },

    impact: {
      score:         impactScore,
      totalStars:    row.stars_on_original_repos || row.star_count || 0,
      forksReceived: row.forks_received          || 0,
      followers:     row.follower_count          || 0,
      density:       impactScore >= 70 ? "High" : impactScore >= 40 ? "Medium" : "Low",
      badges:        deriveImpactBadges(row),
    },

    seniority: {
      score:        seniorityScore,
      label:        row.ai_seniority    || null,
      reviewPct:    Math.round(row.code_review_pct || 0),
      prMergeRate:  Math.round(row.pr_merge_rate   || 0),
      externalPRs:  row.external_prs               || 0,
      tenureYears,
      organizations: row.org_count                 || 0,
      badges:        deriveSeniorityBadges(row, tenureYears),
    },

    scores: {
      activity:      row.activity_score    ?? deriveActivityScore(row),
      credibility:   row.credibility_score ?? 0,
      collaboration: row.collab_score      ?? 0,
      shortlist:     row.ai_shortlist_score ?? 0,
    },

    productionReadiness: {
      cicd:    hasCI,
      tests:   hasTests,
      docker:  hasDocker,
      license: hasLicense,
    },

    projectSpotlight,

    _raw: {
      activity_graph_json:    row.activity_graph_json,
      weekly_activity_json:   row.weekly_activity_json,
      monthly_activity_json:  row.monthly_activity_json,
      contrib_breakdown_json: row.contrib_breakdown_json,
      top5_repos_json:        row.top5_repos_json,
      top10_repos_json:       row.top10_repos_json,
      builder_archetype:      row.builder_archetype,
    },
  };
}

// ── ROUTES ────────────────────────────────────────────────────────────────────

// GET /api/users — paginated list with search, sort, filter
app.get("/api/users", async (req, res) => {
  try {
    const {
      q        = "",
      sort     = "activity",
      tag      = "",
      city     = "",
      language = "",
      status   = "",   // Actively Coding / Recently Coding / Inactive
      seniority = "",  // Senior / Mid-Level / Junior etc
      limit    = 50,
      offset   = 0,
    } = req.query;

    const conditions = ["1=1"];
    const params     = [];

    if (q) {
      conditions.push(`(
        username LIKE ? OR full_name LIKE ? OR company LIKE ? OR
        city LIKE ? OR bio LIKE ? OR
        alltime_languages_json LIKE ? OR alltime_primary_tag LIKE ?
      )`);
      const like = `%${q}%`;
      params.push(like, like, like, like, like, like, like);
    }

    if (tag) {
      conditions.push(`(alltime_primary_tag = ? OR alltime_secondary_tag = ? OR alltime_third_tag = ?)`);
      params.push(tag, tag, tag);
    }

    if (city) {
      conditions.push(`city LIKE ?`);
      params.push(`%${city}%`);
    }

    if (language) {
      conditions.push(`alltime_languages_json LIKE ?`);
      params.push(`%${language}%`);
    }

    if (status) {
      conditions.push(`activity_status = ?`);
      params.push(status);
    }

    if (seniority) {
      conditions.push(`ai_seniority = ?`);
      params.push(seniority);
    }

    const orderMap = {
      activity:    "contrib_365d DESC",
      impact:      "star_count DESC",
      seniority:   "pr_merge_rate DESC",
      shortlist:   "ai_shortlist_score DESC",
      credibility: "credibility_score DESC",
      collab:      "collab_score DESC",
      name:        "full_name ASC",
      stars:       "star_count DESC",
      followers:   "follower_count DESC",
    };
    const orderBy = orderMap[sort] || "ai_shortlist_score DESC";
    const where   = conditions.join(" AND ");

    const [countRow] = await query(
      `SELECT COUNT(*) as total FROM analyzed_users WHERE ${where}`,
      params
    );

    const rows = await query(
      `SELECT * FROM analyzed_users WHERE ${where} ORDER BY ${orderBy} LIMIT ? OFFSET ?`,
      [...params, parseInt(limit), parseInt(offset)]
    );

    res.json({
      total:      countRow.total,
      limit:      parseInt(limit),
      offset:     parseInt(offset),
      developers: rows.map(toDevShape),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/users/:id — single developer by CP-XXXXXX id or username
app.get("/api/users/:id", async (req, res) => {
  try {
    const { id } = req.params;
    let row;

    if (id.startsWith("CP-")) {
      const numericId = parseInt(id.replace("CP-", ""));
      [row] = await query(`SELECT * FROM analyzed_users WHERE id = ?`, [numericId]);
    } else {
      [row] = await query(`SELECT * FROM analyzed_users WHERE username = ?`, [id]);
    }

    if (!row) return res.status(404).json({ error: "Developer not found" });
    res.json(toDevShape(row));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stats — summary stats for the header
app.get("/api/stats", async (req, res) => {
  try {
    const [counts] = await query(`
      SELECT
        COUNT(*) as total,
        COUNT(DISTINCT city) as cities,
        COUNT(DISTINCT alltime_primary_tag) as tags,
        AVG(star_count) as avg_stars,
        SUM(star_count) as total_stars,
        COUNT(CASE WHEN activity_status = 'Actively Coding' THEN 1 END) as active_count,
        COUNT(CASE WHEN ai_shortlist_score >= 65 THEN 1 END) as shortlist_count
      FROM analyzed_users
    `);
    res.json(counts);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/filters — distinct values for filter dropdowns
app.get("/api/filters", async (req, res) => {
  try {
    const cities = await query(
      `SELECT city, COUNT(*) as count FROM analyzed_users WHERE city != '' GROUP BY city ORDER BY count DESC LIMIT 50`
    );
    const tags = await query(
      `SELECT alltime_primary_tag as tag, COUNT(*) as count FROM analyzed_users WHERE alltime_primary_tag IS NOT NULL GROUP BY alltime_primary_tag ORDER BY count DESC`
    );
    const seniorities = await query(
      `SELECT ai_seniority as seniority, COUNT(*) as count FROM analyzed_users WHERE ai_seniority IS NOT NULL GROUP BY ai_seniority ORDER BY count DESC`
    );
    const statuses = await query(
      `SELECT activity_status as status, COUNT(*) as count FROM analyzed_users WHERE activity_status IS NOT NULL GROUP BY activity_status ORDER BY count DESC`
    );
    res.json({ cities, tags, seniorities, statuses });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Health check
app.get("/health", (_, res) => res.json({ ok: true, db: DB_PATH }));

app.listen(PORT, () => {
  console.log(`\n🚀 CodePark API running on port ${PORT}`);
  console.log(`   http://localhost:${PORT}/api/users`);
  console.log(`   http://localhost:${PORT}/api/stats\n`);
});
