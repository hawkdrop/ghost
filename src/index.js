// src/index.js
// GhostScore aggregator (single-file, self-contained)
// Usage: copy .env.example -> .env, fill values, npm install, then `node src/index.js`
//
// Notes:
// - DRY_RUN=true prints payloads but does not POST/PATCH
// - The script tries multiple NocoDB endpoint forms for compatibility
// - Designed to work with GL701 (source) and GL101 (target) table names

import axios from "axios";
import dotenv from "dotenv";
import dayjs from "dayjs";

dotenv.config();

const {
  NOCODB_URL,
  NOCODB_API_KEY,
  SOURCE_TABLE = "GL701",
  TARGET_TABLE = "GL101",
  PAGE_SIZE = 200,
  DRY_RUN = "true",
  RATE_LIMIT_MS = 150
} = process.env;

if (!NOCODB_URL || !NOCODB_API_KEY) {
  console.error("Error: NOCODB_URL and NOCODB_API_KEY must be set in .env");
  process.exit(1);
}

const axiosInstance = axios.create({
  baseURL: NOCODB_URL.replace(/\/$/, ""),
  headers: { "xc-token": NOCODB_API_KEY, "Content-Type": "application/json" },
  timeout: 120000,
});

// ---------- Utilities ----------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function normalizeCompanyKey(name) {
  if (!name) return "unknown";
  return name
    .toString()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\b(ltd|pvt|private|inc|llc|co|company|ltd\.)\b/gi, "")
    .replace(/[^a-z0-9]+/gi, " ")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-");
}

function safeGet(row, key) {
  // NocoDB may return fields directly or under row["Title"], or row?.fields
  if (!row) return "";
  if (row[key] !== undefined && row[key] !== null) return row[key];
  if (row.fields && row.fields[key] !== undefined && row.fields[key] !== null) return row.fields[key];
  // fallback: try case-insensitive
  const all = { ...(row.fields || {}), ...row };
  const found = Object.keys(all).find(k => k.toLowerCase() === key.toLowerCase());
  return found ? all[found] : "";
}

function maskRecruiter(name) {
  if (!name) return "";
  const s = name.toString();
  return s.length <= 4 ? s : s.slice(0, 4) + "****";
}

function topNFromCounts(counts, n = 3) {
  return Object.entries(counts)
    .filter(([k]) => k && k !== "")
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([k]) => k);
}

function getRowId(row) {
  // try common row id fields
  return row?.id ?? row?.ID ?? row?.insertId ?? row?.rowid ?? row?._id ?? row?.row_id ?? null;
}

function pct(count, total) {
  if (!total) return 0;
  return Math.round((count / total) * 1000) / 10; // 1 decimal e.g., 12.3
}

function calculateConfidence(N) {
  // as earlier: min(1, log10(N+1)/log10(11))
  if (!N) return 0;
  const val = Math.log10(N + 1) / Math.log10(11);
  return Math.min(1, Math.max(0, val));
}

function clamp(n, a=0, b=999) {
  return Math.max(a, Math.min(b, n));
}

// ---------- Per-report increment logic (exact weights from your brief) ----------
const STAGE_WEIGHTS = {
  "No Response After Application": 5,
  "No Response After Initial Inquiry": 5,
  "Ghosted After First Interview": 20,
  "Ghosted After Multiple Interviews": 50,
  "Ghosted After Completing an Assignment": 25,
  "Ghosted After Verbal Offer": 60,
};

function reportIncrement(r) {
  let inc = 0;

  const stage = safeGet(r, "Where in the Hiring Process Did Ghosting Happen?");
  inc += STAGE_WEIGHTS[stage] || 0;

  const assignment = safeGet(r, "What Type of Assignment Was Given?");
  const assignmentProvided = assignment && assignment.toString().trim() && !/no assignments required/i.test(assignment);
  if (assignmentProvided) {
    inc += 25;
    const dur = (safeGet(r, "How Long Did It Take You to Complete the Assignment?") || "").toString();
    if (/<\s*2/i.test(dur) || /less than 2/i.test(dur)) inc += 20;
    else if (/2.?[\-–—]?5/i.test(dur) || /2\s*–\s*5/i.test(dur)) inc += 30;
    else if (/5.?[\-–—]?10/i.test(dur) || /5\s*–\s*10/i.test(dur)) inc += 50;
    else if (/more than 10|> ?10/i.test(dur) || /10\+/.test(dur)) inc += 75;
  }

  const paid = (safeGet(r, "Was the interview assignment paid?") || "").toString();
  if (/^\s*no\b/i.test(paid)) inc += 30;

  const feedback = (safeGet(r, "Did You Receive Any Feedback on Your Work?") || "").toString();
  if (/no[, ]*no feedback/i.test(feedback) || /no feedback/i.test(feedback)) inc += 30;
  else if (/vague/i.test(feedback)) inc += 10;

  const receipt = (safeGet(r, "Did They Confirm Receipt of Your Work?") || "").toString();
  if (/^\s*no\b/i.test(receipt)) inc += 10;

  const followup = (safeGet(r, "Did You Follow Up After They Stopped Responding?") || "").toString();
  if (/no response/i.test(followup)) inc += 15;
  else if (/vague excuse/i.test(followup) || /vague/i.test(followup)) inc += 8;

  const wait = (safeGet(r, "How Long Did You Wait Before Realizing You Were Ghosted?") || "").toString();
  if (/<\s*1/i.test(wait) || /less than 1/i.test(wait) || /<\s*1 Week/i.test(wait)) inc += 15;
  else if (/1.?[\-–—]?2/i.test(wait) || /1\s*–\s*2/i.test(wait)) inc += 25;
  else if (/2.?[\-–—]?4/i.test(wait) || /2\s*–\s*4/i.test(wait)) inc += 40;
  else if (/more than 1 month|> ?1 month/i.test(wait) || /more than 1 month/i.test(wait)) inc += 60;

  const rej = (safeGet(r, "Did You Receive an Official Rejection?") || "").toString();
  if (/no[, ]*complete silence/i.test(rej) || /no, complete silence/i.test(rej) || /^\s*no\b/i.test(rej)) inc += 40;

  return inc;
}

// ---------- NocoDB fetch helpers (tries v2 then v1-ish) ----------
async function fetchAllRows(tableName) {
  const limit = Number(PAGE_SIZE) || 200;
  const collected = [];

  // Try V2 style endpoint first
  try {
    let offset = 0;
    while (true) {
      const url = `/api/v2/tables/${encodeURIComponent(tableName)}/rows?limit=${limit}&offset=${offset}`;
      const res = await axiosInstance.get(url);
      const data = res.data?.list ?? res.data?.data ?? res.data;
      if (!Array.isArray(data)) throw new Error("unexpected v2 response format");
      collected.push(...data);
      if (data.length < limit) break;
      offset += limit;
      await sleep(50);
    }
    console.log(`Fetched ${collected.length} rows from ${tableName} (v2)`);
    return collected;
  } catch (err) {
    // fallback to v1 style pagination (page param)
    // console.warn("v2 fetch failed, trying v1 style...", err.message);
  }

  // Try another common shape: /api/v1/tables/{table}/rows?page=1&limit=...
  try {
    let page = 1;
    while (true) {
      const url = `/api/v1/tables/${encodeURIComponent(tableName)}/rows?limit=${limit}&page=${page}`;
      const res = await axiosInstance.get(url);
      const data = res.data?.list ?? res.data?.data ?? res.data;
      if (!Array.isArray(data)) throw new Error("unexpected v1 response format");
      collected.push(...data);
      if (data.length < limit) break;
      page += 1;
      await sleep(50);
    }
    console.log(`Fetched ${collected.length} rows from ${tableName} (v1)`);
    return collected;
  } catch (err) {
    // if both fail, throw informative error
    console.error(`Failed to fetch rows from table ${tableName}. Please verify NOCODB_URL, table name, and API token.`);
    throw err;
  }
}

async function postRowV2(tableName, payload) {
  // try both payload shapes (direct & { row: payload })
  const urlV2 = `/api/v2/tables/${encodeURIComponent(tableName)}/rows`;
  try {
    const r1 = await axiosInstance.post(urlV2, payload);
    return r1.data;
  } catch (err) {
    try {
      const r2 = await axiosInstance.post(urlV2, { row: payload });
      return r2.data;
    } catch (err2) {
      throw err2;
    }
  }
}

async function patchRowV2(tableName, id, payload) {
  const urlV2 = `/api/v2/tables/${encodeURIComponent(tableName)}/rows/${id}`;
  try {
    const r1 = await axiosInstance.patch(urlV2, payload);
    return r1.data;
  } catch (err) {
    try {
      const r2 = await axiosInstance.patch(urlV2, { row: payload });
      return r2.data;
    } catch (err2) {
      throw err2;
    }
  }
}

// fallback POST (v1 style)
async function postRowV1(tableName, payload) {
  const url = `/api/v1/tables/${encodeURIComponent(tableName)}/rows`;
  const r = await axiosInstance.post(url, payload);
  return r.data;
}

// ---------- Build target cache ----------
async function buildTargetCache() {
  const rows = await fetchAllRows(TARGET_TABLE);
  const map = {};
  for (const r of rows) {
    const keyRaw = safeGet(r, "Company Key") || safeGet(r, "Company Name") || "";
    const key = normalizeCompanyKey(keyRaw);
    const id = getRowId(r);
    map[key] = { row: r, id };
  }
  return map;
}

// ---------- Upsert function ----------
async function upsertCompany(targetCache, payload) {
  const key = normalizeCompanyKey(payload["Company Key"] || payload["Company Name"] || "");
  const existing = targetCache[key] || null;

  if (DRY_RUN && DRY_RUN.toLowerCase() === "true") {
    console.log("[DRY RUN] Payload for", key, ":", JSON.stringify(payload, null, 2));
    return { dry: true };
  }

  // create or update
  if (existing && existing.id) {
    // update/patch
    try {
      const res = await patchRowV2(TARGET_TABLE, existing.id, payload);
      await sleep(Number(RATE_LIMIT_MS));
      return { updated: true, id: existing.id, res };
    } catch (err) {
      // try v1 fallback
      try {
        const url = `/api/v1/tables/${encodeURIComponent(TARGET_TABLE)}/rows/${existing.id}`;
        const r = await axiosInstance.patch(url, payload);
        await sleep(Number(RATE_LIMIT_MS));
        return { updated: true, id: existing.id, res: r.data };
      } catch (err2) {
        console.error("Failed to patch existing row:", err2.message || err2);
        throw err2;
      }
    }
  } else {
    try {
      const res = await postRowV2(TARGET_TABLE, payload);
      await sleep(Number(RATE_LIMIT_MS));
      return { created: true, res };
    } catch (err) {
      // fallback v1
      try {
        const res2 = await postRowV1(TARGET_TABLE, payload);
        await sleep(Number(RATE_LIMIT_MS));
        return { created: true, res: res2 };
      } catch (err2) {
        console.error("Failed to create row:", err2.message || err2);
        throw err2;
      }
    }
  }
}

// ---------- Main pipeline ----------
async function main() {
  console.log("Starting GhostScore sync", new Date().toISOString());
  const allRows = await fetchAllRows(SOURCE_TABLE);

  // Group by normalized company key
  const groups = {};
  for (const row of allRows) {
    const companyName = (safeGet(row, "Title") || safeGet(row, "Company Name") || "unknown").toString();
    const key = normalizeCompanyKey(companyName);
    if (!groups[key]) groups[key] = { companyName, rows: [] };
    groups[key].rows.push(row);
  }

  // Build cache of existing GL101 rows for upsert
  const targetCache = await buildTargetCache();

  // Process each company
  for (const [companyKey, group] of Object.entries(groups)) {
    const rows = group.rows;
    const N = rows.length;

    // Stats trackers
    const stats = {
      sumIncrements: 0,
      stageCounts: {},
      assignmentGivenCount: 0,
      unpaidCount: 0,
      receiptYesCount: 0,
      feedbackYesCount: 0,
      feedbackNoCount: 0,
      followupNoResponseCount: 0,
      followupVagueCount: 0,
      officialRejectionCount: 0,
      aiScreeningCount: 0,
      applyAgainYesCount: 0,
      recommendYesCount: 0,
      roleCounts: {},
      recruiterCounts: {},
      assignmentTypeCounts: {},
      locationCounts: {},
      impactCounts: {},
      firstDate: null,
      lastDate: null,
    };

    for (const r of rows) {
      // increment calculation
      const inc = reportIncrement(r);
      stats.sumIncrements += inc;

      // stage counts
      const stage = safeGet(r, "Where in the Hiring Process Did Ghosting Happen?") || "";
      stats.stageCounts[stage] = (stats.stageCounts[stage] || 0) + 1;

      const assignment = safeGet(r, "What Type of Assignment Was Given?") || safeGet(r, "Other Assignment Type") || "";
      if (assignment && !/no assignments required/i.test(assignment)) stats.assignmentGivenCount++;

      const paid = (safeGet(r, "Was the interview assignment paid?") || "").toString();
      if (/^\s*no\b/i.test(paid)) stats.unpaidCount++;

      const receipt = (safeGet(r, "Did They Confirm Receipt of Your Work?") || "").toString();
      if (/^\s*yes\b/i.test(receipt)) stats.receiptYesCount++;

      const feedback = (safeGet(r, "Did You Receive Any Feedback on Your Work?") || "").toString();
      if (/^\s*no\b/i.test(feedback) || /no feedback/i.test(feedback)) stats.feedbackNoCount++;
      else if (/^\s*yes\b/i.test(feedback) || /yes/i.test(feedback)) {
        // classify vague vs detailed by keyword
        if (/vague/i.test(feedback)) stats.feedbackYesCount += 0; // counted as vague by other field mapping
        else stats.feedbackYesCount++;
      }

      const followup = (safeGet(r, "Did You Follow Up After They Stopped Responding?") || "").toString();
      if (/no response/i.test(followup)) stats.followupNoResponseCount++;
      else if (/vague/i.test(followup)) stats.followupVagueCount++;

      const rej = (safeGet(r, "Did You Receive an Official Rejection?") || "").toString();
      if (/^\s*yes\b/i.test(rej) || /formal email/i.test(rej)) stats.officialRejectionCount++;

      const ai = (safeGet(r, "Did the Company Require AI Screening Before Any Interview?") || "").toString();
      if (/^\s*yes\b/i.test(ai)) stats.aiScreeningCount++;

      const again = (safeGet(r, "Would You Apply to This Company Again?") || "").toString();
      if (/^\s*yes\b/i.test(again)) stats.applyAgainYesCount++;

      const rec = (safeGet(r, "Would You Recommend This Employer?") || "").toString();
      if (/^\s*yes\b/i.test(rec)) stats.recommendYesCount++;

      const role = (safeGet(r, "Job Role Applied For") || safeGet(r, "Other Role") || "").toString();
      if (role) stats.roleCounts[role] = (stats.roleCounts[role] || 0) + 1;

      const recName = (safeGet(r, "Recruiter Name or Email (Optional)") || "").toString();
      if (recName) stats.recruiterCounts[recName] = (stats.recruiterCounts[recName] || 0) + 1;

      const at = assignment || "";
      if (at) stats.assignmentTypeCounts[at] = (stats.assignmentTypeCounts[at] || 0) + 1;

      const loc = (safeGet(r, "Company Location") || safeGet(r, "Other Location") || "").toString();
      if (loc) stats.locationCounts[loc] = (stats.locationCounts[loc] || 0) + 1;

      const impact = (safeGet(r, "How Did This Ghosting Experience Affect You?") || "").toString();
      if (impact) {
        const items = impact.toString().split(",").map(x => x.trim()).filter(Boolean);
        for (const it of items) stats.impactCounts[it] = (stats.impactCounts[it] || 0) + 1;
      }

      // created date — NocoDB variants
      const created = r._created_at || r.createdAt || r.created_at || r.created;
      if (created) {
        const d = new Date(created);
        if (!stats.firstDate || d < stats.firstDate) stats.firstDate = d;
        if (!stats.lastDate || d > stats.lastDate) stats.lastDate = d;
      }
    } // end loop rows

    // Aggregation & GhostScore formula (baseline 500 + damping)
    const sum_increments = stats.sumIncrements;
    const damping = N / (N + 1); // prevents single-report explosion
    const normalized_increment = sum_increments * damping;
    const raw_score = 500 + normalized_increment;
    const ghostScore = clamp(Math.round(raw_score), 0, 999);
    const avg_increment = N ? Math.round((sum_increments / N) * 10) / 10 : 0;

    const noResponseCount = (stats.stageCounts["No Response After Application"] || 0) + (stats.stageCounts["No Response After Initial Inquiry"] || 0);
    const ghostedAfterAssignmentCount = stats.stageCounts["Ghosted After Completing an Assignment"] || 0;
    const ghostedAfterInterviewCount = (stats.stageCounts["Ghosted After First Interview"] || 0) + (stats.stageCounts["Ghosted After Multiple Interviews"] || 0);
    const ghostedAfterOfferCount = stats.stageCounts["Ghosted After Verbal Offer"] || 0;

    // Build payload for GL101 — keys must match your GL101 column names exactly
    const payload = {
      "Company Key": companyKey,
      "Company Name": group.companyName,
      "Reports Count": N,
      "Sum Increments": Math.round(sum_increments),
      "Avg Report Increment": avg_increment,
      "GhostScore": ghostScore,
      "First Report Date": stats.firstDate ? stats.firstDate.toISOString() : null,
      "Last Report Date": stats.lastDate ? stats.lastDate.toISOString() : null,
      "No Response %": pct(noResponseCount, N),
      "Ghosted After Assignment %": pct(ghostedAfterAssignmentCount, N),
      "Ghosted After Interview %": pct(ghostedAfterInterviewCount, N),
      "Ghosted After Offer %": pct(ghostedAfterOfferCount, N),
      "Assignments Given %": pct(stats.assignmentGivenCount, N),
      "Unpaid Assignment %": pct(stats.unpaidCount, N),
      "Confirmed Receipt %": pct(stats.receiptYesCount, N),
      "Feedback Received %": pct(stats.feedbackYesCount, N),
      "No Feedback %": pct(stats.feedbackNoCount, N),
      "Follow-up No Response %": pct(stats.followupNoResponseCount, N),
      "Official Rejection %": pct(stats.officialRejectionCount, N),
      "AI Screening Required %": pct(stats.aiScreeningCount, N),
      "Would Apply Again %": pct(stats.applyAgainYesCount, N),
      "Would Recommend %": pct(stats.recommendYesCount, N),
      "Top 3 Roles": topNFromCounts(stats.roleCounts, 3).join(", "),
      "Top 3 Recruiters": topNFromCounts(stats.recruiterCounts, 3).map(maskRecruiter).join(", "),
      "Top 3 Assignment Types": topNFromCounts(stats.assignmentTypeCounts, 3).join(", "),
      "Top 3 Locations": topNFromCounts(stats.locationCounts, 3).join(", "),
      "Common Impact": topNFromCounts(stats.impactCounts, 1).join(", "),
      "Data Quality Flag": N === 1 ? "low-evidence" : "ok",
      "Confidence Score": Math.round(calculateConfidence(N) * 1000) / 1000,
      "Notes": ""
    };

    try {
      const res = await upsertCompany(targetCache, payload);
      console.log(`Upserted ${companyKey}: GhostScore=${ghostScore} reports=${N}`, res);
      // update local cache if created
      if (res?.created) {
        // try to refresh targetCache next loop if necessary, or add minimal entry
        // safe to leave — end of script we do not depend on further updates for process
      }
    } catch (err) {
      console.error("Upsert failed for", companyKey, err.message || err);
    }
  } // end companies loop

  console.log("Done.");
}

main().catch(err => {
  console.error("Fatal error:", err && (err.message || err));
  process.exit(1);
});
